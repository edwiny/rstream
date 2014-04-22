#!/usr/local/bin/perl -w

# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself. 
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

use Local::Rstream::Netlib;
use Local::Rstream::IOBuffer;
use Local::Rstream::JSONLite;
use Fcntl;
use Digest::SHA;
use Getopt::Long           qw(:config no_ignore_case);
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use IO::Compress::Gzip     qw(gzip $GzipError) ;
use String::Glob::Permute  qw(string_glob_permute);


use constant NET_BLOCK_SIZE             => 4096;
use constant FILE_BLOCK_SIZE            => 8192;
use constant BUFFER_SIZE                => 4096000; 
use constant CONNECT_RETRY_TIME         => 5;  # seconds
use constant MAX_HEADER_LEN             => 256; 

use constant STREAM_STATE_FAIL          => -1;
use constant STREAM_STATE_NOT_REQUESTED =>  0;
use constant STREAM_STATE_REQUESTED     =>  1;
use constant STREAM_STATE_IN_PROGRESS   =>  2;
use constant STREAM_STATE_COMPLETE      =>  3;

#----------------------------------------------------------------------------#
# Globals
#----------------------------------------------------------------------------#
$g_helpmsg = qq(
    rstream replicates a fileset at a source machine to multiple target
    destinations with updates in near realtime. It is optimised for 
    distributing incremental changes to files (e.g log files).

    It's a bit like tail -f and rsync commbined (although it cannot allude to
    any of the efficiencies that rsync offers).

    A rstream instance needs to run at source and destination. The instance
    at the source can distribute to multiple destinations, while the one at
    the destination can subscribe to updates from multiple sources.

    Usage: 

    Server mode (run at source of data):
       rstream -l [args]

    Client mode (run at target destination):
       rstream [args] sourcehost1 ...
    
     Options:
      -l --listen         : run in server mode
      -P --port portnum   : port to listen on or connect to
      -d --directory path : root of shared fileset
      -r --regex pattern  : only share files with names matching this
      -s --stdout         : copy data to stdout (client mode)
      -z --compress       : use gzip compression for data in transit
      -c --checksum       : use SHA-1 digests to detect changes to files
                             Warning: slow for large files, or large numbers 
			     of files, or files that update frequently
      -h --help           : display this help
      -p --pidfile        : path to pid file, default is /var/run/rstream.pid
      -f --foreground     : run in foreground
      -v --verbose        : increase verbosity of log messages 
                             (specify multiple times to increase)
    );


%g_clients = ();      # hash containing client sockets and io buffers
                      # manipulation is via the 'client_*' functions
%g_svr_files = ();        # hash containing file names and their state
                          #  'fh'        => the perl filehandle 
                          #  'size'      => size of file
                          #  'curpos'    => current streaming position
                          #  'mtime'     => last detected mtime
                          #  'changed'   => 1,
                          #  'followers' => list of network sockets of clients the file is streamed to
                          #  'next_scan' => scheduled time of next scan for file growth
@g_downloads = ();    # active downloads
                         # fh - filehandle
                         # curpos 
                         # fd - client socket


$g_filelist_changed = 0; # flag used to send fresh file lists

@g_stat_queue = ();   # list of filenames to stat



#----------------------------------------------------------------------------#
# Configuration - global
#----------------------------------------------------------------------------#

$g_conf_loglevel     = 0;   # 0 critical, 1 notice, 2 debug, 3 more debug
$g_conf_path         = ".";
$g_conf_port         = 4096;
$g_conf_working_dir  = ".";
$g_conf_copy_stdout  = 0;
$g_conf_compress     = 0;
$g_conf_checksums    = 0;
$g_conf_pidfile      = "/var/run/rstream.pid";
$g_conf_foreground   = 0;
#----------------------------------------------------------------------------#
# Configuration - server mode
#----------------------------------------------------------------------------#
$g_conf_filepattern  = ".*";

#----------------------------------------------------------------------------#
# Configuration - client mode
#----------------------------------------------------------------------------#
@g_conf_servers      = ();    # where to connect to        



#----------------------------------------------------------------------------#
# Common utility functions
#----------------------------------------------------------------------------#

sub log_msg {
  my $level = shift @_;
  my $line  = shift @_;

  if($level <= $g_conf_loglevel) {
    print STDERR "$line\n";
  }
}

sub daemonize {
  use POSIX;
  POSIX::setsid or die "setsid: $!";
  my $pid = fork ();
  if ($pid < 0) {
    die "fork: $!";
  } elsif ($pid) {
    exit 0;
  }
  chdir "/";
  umask 0;

  close(STDIN);
  close(STDERR);
  close(STDOUT);
  open (STDIN, "</dev/null");
  open (STDOUT, ">/dev/null");
  open (STDERR, ">&STDOUT");
}

sub make_path {
  my $path = shift;
  my $base = "";
  my $success = 0;

  my @dirs = split(/\//, $path);
  for my $d (@dirs) {
    if($d) { 
      $base .= $d;
      $success = mkdir($base);
    }
    $base .= "/";
  }
  return($success);
}

#============================================================================#
# SERVER BIT
#============================================================================#

#----------------------------------------------------------------------------#
# functions to manipulate the client connection data structure
#----------------------------------------------------------------------------#

#
sub client_queue_filedata {
  my $fd     = shift @_;    # client socket fd
  my $fname  = shift @_;    # filename
  my $offset = shift @_;    # offset in file
  my $size   = shift @_;    # data length
  my $dataref= shift @_;    # reference to the data
  my $tmpref = $dataref;
  
  my $hdr = "{\"p\":\"b\",\"f\":\"$fname\",\"o\":\"$offset\",\"s\":\"$size\"}";
  my $zipped_data = '';

  if($g_conf_compress) {
    my $status = gzip($tmpref => \$zipped_data);
    if(!$status) {
      log_msg(0, "Failed to compress block on $fname");
    }
    else {
      $tmpref = \$zipped_data;
      $size   = length($zipped_data);
      $hdr = "{\"p\":\"b\",\"f\":\"$fname\",\"o\":\"$offset\",\"s\":\"$size\",\"z\":\"1\"}";
    }
  }
  client_queue_data($fd, $hdr);
  client_queue_data($fd, $$tmpref, $size);
}


# todo: pass data by ref
sub client_queue_data {
  my $fd = shift;
  return 0 if(!exists($g_clients{$fd}));

  my $data = shift @_;
  if(@_) { $len = shift @_; } else { $len = length($data); }
  $g_clients{$fd}{'writebuf'}->add($data, $len);

}

sub client_has_queued_data {
  my $fd = shift;
  my $len;
  return 0 if(!exists($g_clients{$fd}));
  $len = $g_clients{$fd}{'writebuf'}->len();
  if($len > 0) {
    log_msg(4, "client_has_queued_data: $fd has $len bytes pending");
  }
  return($len);
}

sub client_get_queued_data {
  my $fd = shift;
  my $data;
  return 0 if(!exists($g_clients{$fd}));
  $data = $g_clients{$fd}{'writebuf'}->get(NET_BLOCK_SIZE);
  return($data);
}


#----------------------------------------------------------------------------#
# functions to manage the input files
#----------------------------------------------------------------------------#

sub calc_checksum {
  my $filename  = shift @_;    # name and path of file
  my $offset    = shift @_;    # calc hash over first $offset bytes
  my $sha;
  if(scalar(@_) > 0) {         # optional sha object
    $sha = shift @_;
  }
  else {
     $sha = Digest::SHA->new(1);
  }

  my $FD;

  log_msg(3, "calc_checksum: [$filename]");

  my ($ret, $blocksize, $bytes_read, $block, $filedata);

  $bytes_read = 0;
 
  if(!sysopen($FD, $filename, O_RDONLY)) {
    log_msg(0, "calc_checksum: Failed to open $filename");
    return(undef);
  }

  while($bytes_read < $offset) {
    $blocksize = $offset - $bytes_read;
    $blocksize = FILE_BLOCK_SIZE if($blocksize > FILE_BLOCK_SIZE);
    $ret = sysread($FD, $block, $blocksize);
    if(!$ret) {
      close($FD);
      log_msg(0, "calc_checksum: Failed to read $blocksize bytes at offset $bytes_read on file $filename");
      return(undef);
    }
    $bytes_read += $ret;
    $sha->add($block);
  }
  close($FD);
  my $hash = $sha->hexdigest();
  return($hash);
}

# Check for file deletions, truncations, or replacements.
# to prevent a lot of time being spent in this function if the fileset is large,
# we limit the number of files we process in each iteration of the mainloop.
sub files_process_stat_queue {
  for my $i ( 0 .. 50 ) {
    last if(!scalar(@g_stat_queue));
    my $filename = shift @g_stat_queue;

    my $f = $g_svr_files{$filename};

    my ($nlink, $cursize, $curmtime) = (stat($f->{'fh'}))[3,7,9];

    # deleted files
    if(!$nlink) {
      log_msg(1, "File has been unlinked [$filename]");
      if(defined($f->{'fh'})) {
	  close($f->{'fh'});
      }
      delete($g_svr_files{$filename});
      push @g_svr_files_deleted, $filename;
      $g_filelist_changed++;
      next;
    }

    # truncated files - communicate new size to client
    if($cursize < $f->{'size'}) {
      files_cancel_followers($f);
      $f->{'size'} = $cursize;
      $f->{'mtime'} = $curmtime;
      log_msg(1, "File has been truncated - [$filename]");
      if($g_conf_checksums) {
	$f->{'sha'}->reset();
	$f->{'hash'} = calc_checksum($filename, $cursize, $f->{'sha'});
      }
      $g_filelist_changed++;
      next;
    }

    # recalc and cmpare hash if mtime has changed,
    # except if file is being 'followed' by clients, in which
    # case the checksum will be updated there
    if($curmtime > $f->{'mtime'})
    {
      if($cursize > $f->{'size'} && scalar(@{$f->{'followers'}}) > 0) {
	log_msg(2, "files_process_stat_queue: mtime change detected, but file being streamed [$filename] " .
	    scalar(localtime($curmtime)) ."/" . scalar(localtime($f->{'mtime'})));
	next;
      }

      if($g_conf_checksums) {
	my $old_hash = $f->{'hash'};
	$f->{'sha'}->reset();
	$f->{'hash'} = calc_checksum($filename, $cursize, $f->{'sha'});
	log_msg(1, "Detected changed hash of file [$filename]");
	if($old_hash ne $f->{'hash'}) {
	  files_cancel_followers($f);
	  $g_filelist_changed++;
	}
      } else {    # not configured for checksums, and not currently streaming this file
	files_cancel_followers($f);
	$g_filelist_changed++;
	log_msg(1, "Detected changed mtime on file [$filename] " . scalar(localtime($curmtime)) ."/" . scalar(localtime($f->{'mtime'})) . 
	    " num followers: " . scalar(@{$f->{'followers'}}));
      }
    }
    $f->{'mtime'} = $curmtime;
    $f->{'size'} = $cursize;
  }
}

# refresh our file list to see if files were added or deleted
sub files_find {
    my $path = shift;
    my $pattern = shift;
    my @dirs = ();
    my $file;
    my $fullname;
    local *DIR;
    opendir(DIR, $path) or return(undef);
    while($file = readdir(DIR)) {
        next if($file =~ /^\./);
	$fullname = "$path/$file";
	if(-d $fullname) {
	  push(@dirs, "$fullname");
          log_msg(4, "files_find: checking [$path] [$fullname]");
	  next;
	}
        if($file =~ /$pattern/) {
	  if(!exists($g_svr_files{$fullname})) {
            files_add_file($fullname);
	    $g_filelist_changed++;
	  }
        } 
    }
    closedir(DIR);
    # recurse dirs now
    while($file = shift(@dirs))
    {
        files_find("$file", $pattern);
    }
    return(undef);
}

sub files_refresh {
  my $path = shift;
  my $pattern = shift;

  # only scan for new files if there are no pending delete notifications
  files_find($path, $pattern) if(!scalar(@g_svr_files_deleted));
  @g_stat_queue = keys %g_svr_files;

}

# add a file to our data structure, and prepare it for reading
sub files_add_file {
  my $filename = shift;

  local $FD;
  my ($size, $mtime) = (stat($filename))[7,9];
  if(!defined($size)) {
    log_msg(0, "Error adding [$filename] - cannot stat file - $!");
    return(undef);
  }

  # todo: only add it if it permissions allow read
  if(!sysopen($FD, $filename, O_RDONLY)) {
    log_msg(3, "Failed to open $filename");
    return(undef);
  }

  # initialise the file data structure
  $g_svr_files{$filename} = { 'fh'    => $FD, 
                          'size'      =>  $size, 
			  'curpos'    => 0,
			  'mtime'     => $mtime,
			  'changed'   => 1,
			  'followers' => [],
			  'next_scan' => 0
			};

  log_msg(1, "Adding [$filename] fd " . fileno($FD) . " size $size mtime $mtime");

  if($g_conf_checksums) {
    my $sha = Digest::SHA->new(1);
    $g_svr_files{$filename}{'hash'} = calc_checksum($filename, $size, $sha);
    $g_svr_files{$filename}{'sha'} = $sha;
  }
}

sub files_cancel_followers {
  my $f = shift @_;    # ref to g_svr_files entry

  @{$f->{'followers'}} = ();
  $f->{'curpos'}     = 0;
  $f->{'changed'}    = 1;
}

sub files_scan_new_data {
  my $hashref = shift;
  my $now     = shift;
  my ($filename, $f);

  while (($filename, $f) = (each %$hashref)) {
    my ($data, $offset);

    # only do something if we have clients subscribed to this file
    next if(!exists($f->{'followers'}) || !scalar(@{$f->{'followers'}})); 
    next if($f->{'next_scan'} > $now);

    $offset = $f->{'curpos'};
    my $pos = sysseek($f->{'fh'}, $f->{'curpos'}, 0);  #0 SEEK_SET
    if(!$pos) {
      log_msg(0, "files_scan_new_data: Failed to seek on [$filename]");
      next;
    }
    my $ret = sysread($f->{'fh'}, $data, FILE_BLOCK_SIZE);
    next if(!$ret); # no new data available
    $f->{'next_scan'} = 0;  # if there's data in this iteration, chances are good there will be data in the next

    # work out if data before offset has changed - if so, client neeeds
    # to download the entire file again  

    # todo: this is inefficient and needs to be fixed - we shouldn't have
    # to recalc the hash across the entire file for every block we read!
    if($g_conf_checksums) {
      my $fresh_sha  = Digest::SHA->new(1);
      my $fresh_hash = calc_checksum($filename, $f->{'curpos'}, $fresh_sha);
      if($fresh_hash ne $f->{'hash'}) {
	log_msg(1, "files_scan_new_data: file has been replaced [$filename]");
        # cancel all followers and let recalc of hash happen in process_stat_queue()
	files_cancel_followers($f);
	next;
      }
      # update the hash
      $f->{'sha'}->add($data);
      $f->{'hash'} = $f->{'sha'}->hexdigest();
    }

    $f->{'mtime'} = time();
    
    $f->{'curpos'} = sysseek($f->{'fh'}, 0, 1);       #1 SEEK_CUR
    log_msg(4, "files_scan_new_data: [$filename] last pos [$pos] cur pos " .
	       "[$f->{'curpos'}] read [$ret] bytes (actual length is " . 
	       length($data) . ")");

    # add the data to the buffers of the clients subscribed to this file
    my ($client_fd, $c);
    for(my $i = 0; $i < scalar(@{$f->{'followers'}}); $i++) {
      $client_fd = $f->{'followers'}[$i];
      next if(!defined($client_fd));
      $c = $g_clients{$client_fd};
      if($c->{'writebuf'}->space() < ($ret + MAX_HEADER_LEN)) {
	log_msg(0, "files_scan_new_data: dropping $ret bytes for client $client_fd");
	next;
      }
      client_queue_filedata($client_fd, $filename, $offset, $ret, \$data);
    }
  }
}


#----------------------------------------------------------------------------#
# functions to manage the client interaction
#----------------------------------------------------------------------------#

# remove a client from all our data structures
sub remove_client_fd {
  my $fd = shift @_;
  delete($g_clients{$fd});

  my ($k, %v);
  # remove from g_svr_files
  while (($k, $v) = each (%g_svr_files)) {
    next if(!$v->{'followers'});
    for(my $i = 0; $i < scalar(@{$v->{'followers'}}); $i++) {
      if($fd == $v->{'followers'}[$i]) {
	log_msg(2, "remove_client_fd: found $fd in followers list");
	delete($v->{'followers'}[$i]);
      }
    }
  }
  # remove from downloads
  for(my $i = 0; $i < scalar(@g_downloads); $i++) {
    
    if(defined($g_downloads[$i]) && exists($g_downloads[$i]->{'fd'}) && $fd == $g_downloads[$i]->{'fd'}) {
      log_msg(2, "remove_client_fd: found $fd in downloads list");
      delete($g_downloads[$i]);
    }
  }
}


sub generate_file_list {
  my $only_changed = shift @_;    # only send files that have changed

  my (%list, $filename, $file_val);
  while (($filename, $file_val) = each %g_svr_files) {
    next if($only_changed && !$file_val->{'changed'});
    my ($nlink, $cursize, $curmtime) = (stat($file_val->{'fh'}))[3,7,9];
    next if(!defined($nlink) || !$nlink);
    $file_val->{'size'} = $cursize;

    $list{$filename}  = { 
			  's' => $file_val->{'size'},
			};
    if($g_conf_checksums && $file_val->{'hash'}) {
      $list{$filename}{'c'} = $file_val->{'hash'};
    }

    $file_val->{'changed'} = 0;

  }
  if($only_changed) {
    while($filename = shift @g_svr_files_deleted) {
      $list{$filename} = { 
	's' => -1
      };
    }

  }
  return(Y7::JSONLite::encode_hash_to_json(\%list));
  
}

sub queue_list_update { 

  %resp_hdr = ( "p" => "lp", 's' => 0, "st" => 1 ); 
  my $resp_data = generate_file_list(1);
  $resp_hdr{'s'} = length($resp_data);
  while (my ($fd, $c) = each(%g_clients)) {
    client_queue_data($fd, Y7::JSONLite::encode_hash_to_json(\%resp_hdr));
    client_queue_data($fd, $resp_data, $resp_hdr{'s'});
  }
  log_msg(1, "File list changed - sent update");
}

sub process_client_list_request {
  my $f = shift @_;
  my $req_ref = shift @_;
  my $resp_hdr_ref = shift @_;
  my $data = generate_file_list(0);
  $resp_hdr_ref->{'p'} = "l";
  $resp_hdr_ref->{'st'} = 1;
  return($data);
}

# todo: block requests unimplemented
sub process_client_block_request {
  my $f = shift @_;
  my $req_ref = shift @_;
  my $resp_hdr_ref = shift @_;
  my ($data);

  return($data);
}

sub process_client_stream_request {
  my $f = shift @_;
  my $req_ref = shift @_;
  my $resp_hdr_ref = shift @_;
  my ($data, $filename, $offset);

  $resp_hdr_ref->{'p'}  = "s";
  $offset = 0;
  if(!exists($req_ref->{'f'})) { 
    $resp_hdr_ref->{'st'} = STREAM_STATE_FAIL;
    log_msg(1, "process_client_stream_request: filename missing from request");
    return(undef);
  }
  $filename = $req_ref->{'f'};

  if(exists($req_ref->{'o'})) { 
    $offset = $req_ref->{'o'};
  }

  my $ret = add_new_download($f, $filename, $offset);
  if(!$ret) {
    $resp_hdr_ref->{'st'} = STREAM_STATE_FAIL;
    log_msg(1, "process_client_stream_request: Failed to read file $filename");
    return(undef);
  }
  
  $resp_hdr_ref->{'st'} = STREAM_STATE_IN_PROGRESS;
  $resp_hdr_ref->{'f'}  = $filename;
  return(undef);
}



# todo: copy request args into response so client can match up responses with requests
sub process_client_request {
  # todo: sanitise / yiv filter all input args
  # esp. strip ../ from filenames
 
  my $f = shift @_;                        # client socket fd, also key intoa %g_clients
  my $readbuf = $g_clients{$f}{'readbuf'};
  my $datasize = $readbuf->len();
  my ($req_text, $tmppos, $req_hash, $resp_data, %resp_hdr);

  # check if we have a complete request by looking for a '}' 
  $tmppos = index($readbuf->peek(), '}');
  if($tmppos == -1) {
    log_msg(3, "process_client_request: failed to read complete packet");
    # just return at this point and allow time for more data to accumulate in the read buffer
    return(0);
  }
  $req_text = $readbuf->get($tmppos + 1);
  log_msg(3, "process_client_request: got complete request [$req_text]");
  $req_hash = Y7::JSONLite::decode_hash_from_json($req_text);
  # initialise the hash contonain the response header
  %resp_hdr = ( 's' => 0, "st" => 0 );  # initliase status to fail (0)
  $resp_data = '';

  if(!$req_hash || !exists($req_hash->{"cmd"}) || ! $req_hash->{'cmd'}) {
      $resp_data = "Failed to parse request";
      $resp_hdr{'s'} = length($resp_data);
  } else { 

    if($req_hash->{'cmd'} eq "LIST") {
      $resp_data = process_client_list_request($f, $req_hash, \%resp_hdr);
    } elsif ($req_hash->{'cmd'} eq "BLOCK" ) {
      $resp_data = process_client_block_request($f, $req_hash, \%resp_hdr);
    } elsif ($req_hash->{'cmd'} eq "STREAM" ) {
      $resp_data = process_client_stream_request($f, $req_hash, \%resp_hdr);
    } else {
      # todo
    }

  }

  if($resp_data) {
    $resp_hdr{'s'} = length($resp_data);
  }
  client_queue_data($f, Y7::JSONLite::encode_hash_to_json(\%resp_hdr));
  if($resp_data) {
    client_queue_data($f , $resp_data, $resp_hdr{'s'});
  }
}



sub add_new_download {     # return 1 if success
  my $client = shift @_;   # client socket
  my $filename = shift @_; # file being downloaded
  my $pos = shift @_;      # offset to start at
  
  my $f = $g_svr_files{$filename};
  if(!$f) { 
    log_msg(1, "add_new_download: no such file [$filename]");
    return(0);
  }
  if(!$f->{'fh'}) {
    log_msg(1, "add_new_download: not ready [$filename]");
    return(0);
  }

  push @g_downloads, { 'filename' => $filename, 'fh' => $f->{'fh'}, 'curpos' => $pos, 'client' => $client };
  return(1);
}

sub process_downloads {  # return true if we actually did something

  my $data = '';
  my $done_something = 0;
  my $offset;


  for(my $i = 0; $i < scalar(@g_downloads); $i++) {
    $d = $g_downloads[$i];
    next if (!$d);
    next if (!$g_clients{$d->{'client'}});
    # check if client buffer isn't full

    my $readsize = $g_clients{$d->{'client'}}{'writebuf'}->space();
    if($readsize < 2*FILE_BLOCK_SIZE) {      # client buffer is full, don't read any more
      log_msg(2, "process_downloads: client $d->{'client'} write buffer full");
      next;
    }

    if($readsize > FILE_BLOCK_SIZE) { $readsize = FILE_BLOCK_SIZE; }
    
    $offset = $d->{'curpos'};  # save offset to send to client

    my $pos = sysseek($d->{'fh'}, $d->{'curpos'}, 0);  #0 SEEK_SET
    if(!$pos) {
      log_msg(0, "process_downloads: Failed to seek on [$d->{filename}]");
      next;
    }
    my $ret = sysread($d->{'fh'}, $data, $readsize);

    if(!defined($ret))  { # ERROR
       log_msg(0, "Error reading file $d->{'filename'}");
       delete $g_downloads[$i];
       next;
    }

    if($ret == 0)  { # EOF
       if((stat($d->{'fh'}))[7] > $d->{'curpos'}) {
	 log_msg(0, "process_downloads: read 0 bytes but not at EOF! [$d->{filename}]");
	 next;
       }
       log_msg(3, "process_downloads: download completed [$d->{filename}]");

       # notify client the download is complete
       client_queue_data($d->{'client'}, Y7::JSONLite::encode_hash_to_json({ "p" => "s", "f" => $d->{'filename'}, "st" => STREAM_STATE_COMPLETE }));



       # we're at the end of file, add to streaming list
       my $f = $g_svr_files{$d->{'filename'}};
# last todo
       if(!scalar(@{$g_svr_files{$d->{'filename'}}->{'followers'}})) {  # first one to follow this file
	 my $f = $g_svr_files{$d->{'filename'}};
         $f->{'followers'} = [ $d->{'client'} ];
         $f->{'curpos'} = $d->{'curpos'};
	 log_msg(2, "process_downloads: client $d->{'client'} first one to follow $d->{'filename'} at pos $f->{'curpos'}");
       }
       else {
	 push(@{$g_svr_files{$d->{'filename'}}{'followers'}}, $d->{'client'});
	 log_msg(2, "process_downloads: client $d->{'client'} added to followers list for $d->{'filename'} at pos $g_svr_files{$d->{'filename'}}{'curpos'}");
       }
       delete $g_downloads[$i];
       next;
    }
    log_msg(4, "process_downloads: queuing $ret bytes of $d->{'filename'} at pos $offset to client $d->{'client'}");
    # push data block onto clients write buffer
    client_queue_filedata($d->{'client'}, $d->{'filename'}, $offset, $ret, \$data);
    $d->{'curpos'}+=$ret;
    $done_something++;
  }
  return($done_something);  
}  


sub do_server {

  my $server = create_server_sock("tcp", $g_conf_port) or die "Failed to create server socket";

#----------------------------------------------------------------------------#
# The main loop. Here we
# - check for events on the sockets
# - write any pending data in buffers to client sockets
# - scan the input directory for new or deleted files
# - check for new data in the files
# - process downlaod requestss
#----------------------------------------------------------------------------#

  my $timer_check_files = 0;

  files_refresh($g_conf_path, $g_conf_filepattern);

  while(!$g_done) {

    my $now = time();
    
    files_process_stat_queue();

    if(scalar(@g_downloads) > 0) { 
      process_downloads();
    }

    # only scan new data if we have clients connected
    if(scalar(keys %g_clients) > 0 ) {
      files_scan_new_data(\%g_svr_files, $now);
    }

    # initialise file handle bitsets

    my (@write_fds, @read_fds);
    @write_fds = (); @read_fds = ();

    my $win  = '';
    my $wout = '';
    my $rin  = '';
    my $rout = '';

    for my $f (keys %g_clients) {

      # only read from a client socket if there is space in its write buffer
      if($g_clients{$f}{'writebuf'}->space() > (FILE_BLOCK_SIZE + MAX_HEADER_LEN)) {
	push(@read_fds, $f);
      }
      
      # check if we have anything to process from the client
      if($g_clients{$f}{'readbuf'}->len()) {
	process_client_request($f);
      }

      if(client_has_queued_data($f)) {
	push(@write_fds, $f);
      }
    }

    $win = set_fbits(@write_fds);

    # always be willing to read from server socket (accepts)
    $rin = set_fbits(fileno($server), @read_fds);

    my $ret = select($rout=$rin, $wout=$win, undef, 0.1);

    if($ret > 0) {
      # check for incoming connection
      if(vec($rout, fileno($server), 1)) {
	my %info = ();
	my $client =  accept_client($server, \%info);
	if(!$client) { log_msg(1, "Failed to accept incoming connection"); next; }
        # todo: add access control here

	log_msg(1, "New connection from [$info{ip}]");

	# initialise the client data structure
	my $tmp = { 'fh' => $client, 
		    'writebuf' => Y7::IOBuffer->new(),
		     'readbuf' => Y7::IOBuffer->new()
		  };
	$tmp->{'writebuf'}->size(BUFFER_SIZE);
	$tmp->{'readbuf'}->size(BUFFER_SIZE);
	$g_clients{fileno($client)} = $tmp;


      }

      # READS
      for my $fd (keys %g_clients) {
	if(vec($rout, $fd, 1)) {
	  
	  my ($ret, $data);
	  $ret = sysread($g_clients{$fd}{'fh'}, $data, NET_BLOCK_SIZE);
	  if(!$ret) { 
	    log_msg(1, "read from socket $fd failed, closing");
	    close($g_clients{$fd}{'fh'});
	    remove_client_fd($fd);
	    next;
	  }
	  log_msg(4, "main: socket $fd read [$data]");
	  $g_clients{$fd}{'readbuf'}->add($data, $ret);
	}
      }

      # WRITES
      for my $f (keys %g_clients) {
	if(vec($wout, $f, 1))  {
	  my $data = client_get_queued_data($f);
	  log_msg(4, "main: socket $f writing " . length($data) . " bytes");
	  my $ret = syswrite($g_clients{$f}{'fh'}, $data);
	  if(!$ret) {
	    log_msg(1, "socket $f has gone away ($!), closing connection");
	    close($g_clients{$f}{'fh'});
	    remove_client_fd($f);
	  }
	  else {
#          print "main: $f successfully wrote $ret bytes to network\n";
	  }
	}
      }
    } # end of select return > 0 
    # refresh the file list, only if there has been no network IO in the current 
    # mainloop iteration
    elsif($now > $timer_check_files) {
      $timer_check_files = $now + 5;
      files_refresh($g_conf_path, $g_conf_filepattern) if (!scalar(@g_stat_queue));
      if($g_filelist_changed) {
	$g_filelist_changed = 0;
	queue_list_update() if (scalar(%g_clients));
      }
    }

  }
}


#============================================================================#
# CLIENT BIT
#============================================================================#

#----------------------------------------------------------------------------#
# Client globals
#----------------------------------------------------------------------------#
my %g_clnt_servers = (); # map server names to sockets, hash of refs
                         # key: server name
                         # fd - socket file descriptor
                         # next_connect - timestamp of next connection attempt
                         # list_received - have we reeived a list response yet

my %g_clnt_socks  = (); # data structure for connnections to remote sources, 
                        # maps socket file descriptor to name and conection 
                        # state data
                         # key: socket fd
                         # fh - perl filehandle
                         # name: server name
                         # readbuf 
                         # writebuf
my %g_clnt_files = ();  # keyed by server name, contains res to hash of the files we are tracking
                         # sstatus - status of stream request
                            # see STREAM_STATE_* constants
                         # stime - timestamp - when stream req was sent
                         # size - size of the file, by our reckoning

my $g_num_downloads = 0;   # number of downloads in progress

use constant MAX_CONCURRENT_DOWNLOADS => 1;



#----------------------------------------------------------------------------#
# 
#----------------------------------------------------------------------------#

sub clnt_files_find {
  my $servername = shift;   # first component of path
  my $path       = shift;   # the rest of the path
  my @dirs = ();
  my $file;
  my $fullname;
  local *DIR;

  opendir(DIR, $path) or return;

  while($file = readdir(DIR)) {
    next if($file =~ /^\./);

    $fullname = "$path/$file";
    if(-d $fullname) {
      push(@dirs, "$fullname");
      next;
    }
    if(-w $fullname) {
      my $cursize = (stat($fullname))[7];
      $g_clnt_files{$servername}{$fullname}{'size'} = $cursize;
      $g_clnt_files{$servername}{$fullname}{'sstatus'} = STREAM_STATE_NOT_REQUESTED;
      $g_clnt_files{$servername}{$fullname}{'stime'} = 0;
      log_msg(1, "starting [$servername:$fullname] at offset $cursize");
    } else {
      log_msg(0, "Local file not writable [$fullname]");
    }
  }
  closedir(DIR);
  # recurse dirs now
  while($file = shift(@dirs))
  {
      clnt_files_find($servername, $file);
  }
  return;
}

sub load_local_file_offsets {
  local *DIR;

  opendir(DIR, ".") or return(0);
  while(my $servername = readdir(DIR)) {
    next if(!-d $servername);
    next if($servername =~ /^\./);
    chdir("$servername") or die "cannot change to [$servername] - $!";
    clnt_files_find($servername, ".");
    chdir("..");
  }
  close(DIR);
  return(1);
}


# map file path received from the server to local config
sub localise_filename {
  my $server   = shift @_;   # name of server we've received the file fomr
  my $filename = shift @_;   # path as received by server
  return("$server/$filename");
}


sub strip_filename {         # remove filename from path
  my $path_and_filename = shift @_;

  my $pos = rindex($path_and_filename, "/");
  if($pos == -1)  { return ($path_and_filename); }
  return(substr($path_and_filename, 0, $pos));
}


sub request_downloads {
  my ($server, $f, $fentry, $filename);

  while (($server, $f) = each(%g_clnt_files)) {
    if(!exists($g_clnt_servers{$server})) { next; }
    next if(!defined($g_clnt_servers{$server}{fd}));
#    if(!exists($g_clnt_socks{$g_clnt_servers{$server}->{fd}})) { next; }
    my $conn = $g_clnt_socks{$g_clnt_servers{$server}->{fd}};

    # only request a download if we've received a list update from this server before
    next if(!$g_clnt_servers{$server}{list_received});

    # only request download if read buffer for this connetion has space
    if($conn->{'readbuf'}->space() < (NET_BLOCK_SIZE * 10)) {
      log_msg(1, "readbuffer full: $server - not requesting downloads from this server");
      next;
    }

    while(($filename, $fentry) = each(%$f)) {
      
      if($fentry->{'sstatus'} == STREAM_STATE_NOT_REQUESTED) {

	my $reqdata  = "{\"cmd\":\"STREAM\"," .
			"\"f\":\"" . $filename . "\"," .
			"\"o\":\"" . $fentry->{'size'} . "\"" .
			"}";

	$conn->{'writebuf'}->add($reqdata);
	$fentry->{'sstatus'} = STREAM_STATE_REQUESTED;
	$fentry->{'stime'} = time();

	log_msg(1, "Requesting stream for [$server:$filename] offset $fentry->{'size'}");
	$g_num_downloads++;
	return if($g_num_downloads >= MAX_CONCURRENT_DOWNLOADS);
      }
    }
  }
}

sub reset_stream_state_for_server {
  my $servername  = shift @_;    # name of server to reset state

  my ($fname, $d);
  log_msg(1, "Resetting stream state for [$servername]");
  $g_clnt_servers{$servername}{list_received} = 0;
  my $local_files = $g_clnt_files{$servername};

  while(($fname, $d) = each(%$local_files)) {
    $d->{'sstatus'} = STREAM_STATE_NOT_REQUESTED;
  }
}

sub process_server_list_response {
  my $conn     = shift @_;      # socket connection
  my $response = shift @_;      # data payload of packet
  my $pkt      = shift @_;      # packet type


  my $response_hash = Y7::JSONLite::decode_hash_from_json($response);
  if(!$response_hash) { 
    log_msg(0, "process_server_list_response: failed to parse JSON");
    return(0);
  }

  log_msg(2, "List update received from $conn->{'name'}");
  log_msg(4, $response);

  my $local_files = $g_clnt_files{$conn->{'name'}};
  $g_clnt_servers{$conn->{'name'}}{list_received} = 1;

  my ($fname, $d);

  if(!$local_files) {
    $g_clnt_files{$conn->{'name'}} =  {};
    $local_files = $g_clnt_files{$conn->{'name'}};
  }

  while(($fname, $d) = each(%$response_hash)) {

    my $rec = undef;
    if(exists($local_files->{$fname})) {
      $rec = $local_files->{$fname};
    }

    # different possible cases
    # 1. client knows about the file ( exists($local_files->{$fname}) )
    #     1.1. remote size is -1     ( $d->{'s'} == -1 )
    #        -> delete file
    #        -> remove from g_clnt_files
    #        -> next
    #     1.2. remote size is smaller than local size    ( $rec->{'size'} > $d->{'s'} )
    #        -> truncate file and set sstatus to NOT_REQUESTED and size to 0 
    #     1.3. hash is different than remote hash
    #        -> truncate file and set sstatus to NOT_REQUESTED and size to 0 
    #     1.4 we're not streaming the file ( $rec->{'sstatus'} == NOT_REQUESTED )
    #        -> request stream from our local offset / size of file
    #        -> next
    # 2. client knows nothing about file
    #     -> create path for it
    #     -> create empty file
    #     -> add to g_clnt_files
    #     -> request stream from our local offset / size of file
    #     -> next


    log_msg(2, "List item: [$conn->{'name'}:$fname]: " . 
				  Y7::JSONLite::encode_hash_to_json($d));
    my $local_filename   = localise_filename($conn->{'name'}, $fname);

    if($rec) { # 1
      if($d->{'s'} == -1) {   # 1.1
	log_msg(1, "Deleted from server: [$conn->{'name'}:$fname]");
        unlink($local_filename);
	delete($local_files->{$fname});
	next;
      }

      if($rec->{'size'} > ($d->{'s'} + NET_BLOCK_SIZE)) {    # 1.2
	log_msg(1, "Restarting [$conn->{'name'}:$fname] because remote file has shrunk (local: " . 
					      $rec->{'size'} .", remote: " . $d->{'s'} . ")");
	$rec->{'size'} = 0;
	$rec->{'sstatus'} = STREAM_STATE_NOT_REQUESTED;
	if(exists($d->{'c'})) {
	  $rec->{'hash'} = $d->{'c'};
	}
        truncate($local_filename, 0);
      }
      elsif(exists($rec->{'hash'}) && exists($d->{'c'}) && 
	    $rec->{'hash'} ne $d->{'c'}) {
	log_msg(1, "Hash dffers for $local_filename");
	$rec->{'size'} = 0;
	$rec->{'sstatus'} = STREAM_STATE_NOT_REQUESTED;
	$rec->{'hash'} = $d->{'c'};
        truncate($local_filename, 0);
      }
    }
    else { # 2
      my $dirname          = strip_filename($local_filename);
      log_msg(3, "Creating path [$dirname]");
      make_path($dirname);
      $local_files->{$fname} = { "size" => 0, 'sstatus' => STREAM_STATE_NOT_REQUESTED };
      if(exists($d->{'c'})) {
	$local_files->{$fname}{"hash"} = $d->{'c'};
      }
    }
  }

  # check if there has been any deletes
  # only if change list was not incremental
  if($pkt eq "l") {
    while(($fname, $d) = each(%$local_files)) {
      if(!exists($response_hash->{$fname})) {
	my $local_filename   = localise_filename($conn->{'name'}, $fname);
	log_msg(1, "Deketed from server: [$conn->{'name'}:$fname]");
	unlink($local_filename) or log_msg(0, "Failed to delete [$local_filename] - $!");
	delete($local_files->{$fname});
      }
    }
  }
  return(1);
}

# todo: we really should be using buffered IO
sub process_server_block_response {
  my $conn     = shift @_;      # g_clnt_socks entry
  my $hdr      = shift @_;      # response header
  my $data     = shift @_;      # data received
  my $fentry;
  my $response = '';


  return if(!exists($hdr->{'f'}));
  my $local_fname = localise_filename($conn->{'name'}, $hdr->{'f'});
  my $len = $hdr->{'s'};
  return if (!$len);

  # check if payload is compressed
  if(exists($hdr->{'z'}) && $hdr->{'z'} eq "1") {
    gunzip(\$data => \$response);
    $len = length($response);
  }
  else {
    $response = $data;
  }

  my $fd;

  log_msg(1, "Block update: [$local_fname] $len bytes");
  if(!exists($g_clnt_files{$conn->{'name'}}{$hdr->{'f'}})) {
    log_msg(0, "Update received on unknown file [$hdr->{'f'}]");
    return(0);
  }
  $fentry = $g_clnt_files{$conn->{'name'}}{$hdr->{'f'}};
  $fentry->{'size'}  += $len;

  if(!sysopen($fd, $local_fname, O_CREAT | O_WRONLY | O_APPEND)) {
    log_msg(1, "Failed to open $local_fname - $!");
    return(0);
  }
  my $ret = syswrite($fd, $response, $len);
  if(!$ret) {
    log_msg(1, "process_server_block_response: failed to write $len bytes to $local_fname");
  }
  if($g_conf_copy_stdout) {
    print $response;
  }
  close($fd);
  return(1);
}

sub process_server_stream_response {
  my $conn     = shift @_;      # g_clnt_socks entry
  my $hdr      = shift @_;      # response header
  my $data     = shift @_;      # data received


  my $filename = $hdr->{f};
  if($hdr->{st} == STREAM_STATE_FAIL) {
    log_msg(1,"Streaming request for [$filename] failed");
# todo - deal with this error
    $g_num_downloads--;
    return(0);
  }

  if(!exists($g_clnt_files{$conn->{'name'}}{$hdr->{'f'}})) {
    log_msg(0, "Update received on unknown file [$hdr->{'f'}]");
    return(0);
  }
  my $fentry = $g_clnt_files{$conn->{'name'}}{$hdr->{'f'}};
  
  if($hdr->{st} == STREAM_STATE_IN_PROGRESS) {
    log_msg(1,"server response: download started for $conn->{name}:[$filename]");
    $fentry{'sstatus'} = STREAM_STATE_IN_PROGRESS;
    
  }
  elsif($hdr->{st} == STREAM_STATE_COMPLETE) {
    log_msg(1,"server response: completed download of $conn->{name}:[$filename]");
    $fentry{'sstatus'} = STREAM_STATE_COMPLETE;
    $g_num_downloads--;
  }
  return(1);
}


# retirn 0 if no more data
sub process_server_response  {
  my $fd       = shift @_;          # socket fd
  my $conn     = $g_clnt_socks{$fd};
  my $readbuf  = $conn->{'readbuf'};
  my $datasize = $readbuf->len();
  my ($hdr_text, $tmppos, $hdr, $resp_data, $datalen);

  return(0) if (!$readbuf->len());
  # check if we have a complete request by looking for a '}' 
  $tmppos = index($readbuf->peek(), '}');
  if($tmppos == -1) {
    log_msg(4, "process_server_response: incomplete header... waiting for more deta to arrive - [" . $readbuf->peek() . "]");
    return(0);
  }
  $hdr_text = $readbuf->get($tmppos + 1);
  $hdr = Y7::JSONLite::decode_hash_from_json($hdr_text);
  if(!$hdr || !defined($hdr->{'p'})) {
    log_msg(0, "process_server_response: failed to parse JSON");
    return(0);
  }
  $datalen = 0;
  $resp_data = undef;
  if(exists($hdr->{'s'})) { 
    $datalen = $hdr->{'s'};
  }

  # retrieve the payload
  if($datalen > 0) {

    if($readbuf->len() < $datalen) {
      log_msg(4, "process_server_response: incomplete data in buffer, pushing back into buffer");
      $readbuf->push($hdr_text, $tmppos + 1);
      return(0);
    }

    log_msg(3, "process_server_response: got response with payload [$hdr_text] [" . substr($readbuf->peek(),0,10) . "...]");

    $resp_data = $readbuf->get($datalen);
  }
  else {
    log_msg(3, "process_server_response: got response [$hdr_text]");
  }

  if($hdr->{'p'} eq "l" || $hdr->{'p'} eq "lp") {
    process_server_list_response($conn, $resp_data, $hdr->{'p'});
  } elsif($hdr->{'p'} eq "b") {
    process_server_block_response($conn, $hdr, $resp_data);
  } elsif($hdr->{'p'} eq "s") {
    process_server_stream_response($conn, $hdr, $resp_data);
  } else {
    log_msg(0, "process_server_response: received unknown response type [$hdr->{'p'}]");
  }
  return(1);
}

#----------------------------------------------------------------------------#
# The client main loop
#----------------------------------------------------------------------------#

sub do_client {

  foreach my $s (@g_conf_servers) {
    $g_clnt_servers{$s} = { 'fd' => undef, 'next_connect' => 0 };
  }

  load_local_file_offsets() || die "Error loading local file offset";


  while(!$g_done) {

    my $now = time();
   
    # connection attempts
    foreach my $s (keys %g_clnt_servers) {
      next if (defined($g_clnt_servers{$s}->{'fd'}));
      next if ($g_clnt_servers{$s}->{'next_connect'} > $now);

      my $server = connect_tcp_sock($s, $g_conf_port); 
      if(!defined($server)) {
	$g_clnt_servers{$s}->{'next_connect'} = $now + CONNECT_RETRY_TIME;
	log_msg(1, "Failed to connect to [$s:$g_conf_port] - $!");
	next;
      }
      $fd = fileno($server);
      $g_clnt_servers{$s}->{'fd'}      = $fd;
      $g_clnt_socks{$fd}->{'fh'}       = $server;
      $g_clnt_socks{$fd}->{'name'}     = $s;
      $g_clnt_socks{$fd}->{'readbuf'}  = Y7::IOBuffer->new();
      $g_clnt_socks{$fd}->{'readbuf'}->size(BUFFER_SIZE);
      $g_clnt_socks{$fd}->{'writebuf'} = Y7::IOBuffer->new();
      $g_clnt_socks{$fd}->{'writebuf'}->size(BUFFER_SIZE);
      
      # immediately queue a LIST request to this server
      $g_clnt_socks{$fd}->{'writebuf'}->add("{\"cmd\":\"LIST\"}");
    }


    request_downloads() if($g_num_downloads < MAX_CONCURRENT_DOWNLOADS);

    my @read_fds = ();

    # only attempt to read if read buffers have space
    for my $f (keys %g_clnt_socks) {
      if($g_clnt_socks{$f}{'readbuf'}->space() > (NET_BLOCK_SIZE * 10)) {
	push(@read_fds, $f);
      }
      else {
	log_msg(2, "read buffer on fd $f out of space!");
      }
    }
    

    my $rin = set_fbits(@read_fds);
    my $rout = '';
    # check if we have anything to write
    my @write_fds;
    for my $f (keys %g_clnt_socks) {
      if($g_clnt_socks{$f}{'writebuf'}->len() && $g_clnt_socks{$f}{'readbuf'}->space() > (NET_BLOCK_SIZE * 10)) {
	push(@write_fds, $f);
      }
    }

    my $win = set_fbits(@write_fds);
    my $wout = '';

    my $ret = select($rout=$rin, $wout=$win, undef, 0.1);

    if($ret > 0) {
      # READS
      for my $fd (keys %g_clnt_socks) {
	if(vec($rout, $fd, 1)) {

	  my ($ret, $data);
	  $ret = sysread($g_clnt_socks{$fd}{'fh'}, $data, NET_BLOCK_SIZE);
	  if(!$ret) { 
	    log_msg(1, "read from socket $fd failed, closing");
	    close($g_clnt_socks{$fd}{'fh'});
	    # todo: flush any data in buffers
	    reset_stream_state_for_server($g_clnt_socks{$fd}->{'name'});
	    $g_clnt_servers{$g_clnt_socks{$fd}->{'name'}}->{'fd'} = undef;
	    delete($g_clnt_socks{$fd});
	    next;
	  }
	  $g_clnt_socks{$fd}{'readbuf'}->add($data, $ret);
	  log_msg(3, "do_client: $fd: Read $ret bytes");
	}
      }

      # WRITES
      for my $fd (keys %g_clnt_socks) {
	if(vec($wout, $fd, 1))  {
	  my $data = $g_clnt_socks{$fd}{'writebuf'}->get(NET_BLOCK_SIZE);
	  my $ret = syswrite($g_clnt_socks{$fd}{'fh'}, $data);
	  if(!$ret) {
	    log_msg(1, "socket $fd has gone away ($!), closing connection");
	    close($g_clnt_socks{$fd}{'fh'});
	    # todo: flush any data in buffers
	    reset_stream_state_for_server($g_clnt_socks{$fd}->{'name'});
	    $g_clnt_servers{$g_clnt_socks{$fd}}->{'fd'} = undef;
	    delete($g_clnt_socks{$fd});
	    next;
	  }
	  my $l = length($data);
	  if($ret != $l) {
	    log_msg(2, "do_client: $fd partial write");
	    $g_clnt_socks{$fd}{'writebuf'}->push(substr($data, $ret, $l - $ret));
	  }
	}
      }
    } # end if($ret > 0)
    foreach my $fd (keys %g_clnt_socks) {
      if($g_clnt_socks{$fd}->{'readbuf'}->len()) {
	while(process_server_response($fd)) { };
      }
    }
  }
}



#----------------------------------------------------------------------------#
# Main program execution starts here
#----------------------------------------------------------------------------#

my $servermode = 0;

my $help = 0;
if(!GetOptions(
      "listen|l"      => \$servermode,
      "port|P=i"      => \$g_conf_port,
      "directory|d=s" => \$g_conf_working_dir,
      "verbose|v+"    => \$g_conf_loglevel,
      "regex|r=s"     => \$g_conf_filepattern,
      "stdout|s"      => \$g_conf_copy_stdout,
      "compress|z"    => \$g_conf_compress,
      "checksum|c"    => \$g_conf_checksums,
      "pidfile|p=s"   => \$g_conf_pidfile,
      "foreground|f"  => \$g_conf_foreground,
      "help|h"        => \$help
      ) || $help) {
# todo: print help text
  print "$g_helpmsg\n";
  exit 1;
}

if(!$servermode) {
  while(my $s = shift @ARGV) {
    push(@g_conf_servers, string_glob_permute($s));
  }
}

sub sighandler {
  my $sig = shift;
  if($sig eq "INT" or
     $sig eq "TERM" or
     $sig eq "QUIT") {
    log_msg(1, "Killed by $sig");
    $g_done = 1;
  }
}

$SIG{INT}  = \&sighandler;
$SIG{TERM} = \&sighandler;
$SIG{QUIT} = \&sighandler;

if(!$g_conf_foreground) {
  if($g_conf_working_dir !~ /^\//) {
    log_msg(0, "Working directory must be specified as absolute path when running in daemon mode");
    exit(1);
  }
  daemonize();
}

chdir($g_conf_working_dir) or die "Failed to change directory to [$g_conf_working_dir]\n";

open(my $pfile, ">$g_conf_pidfile") or die "Failed to open $g_conf_pidfile for writing";
print $pfile $$;
close($pfile);

if($servermode) {
  log_msg(1, "Starting in servermode");
  do_server();
} else {
  log_msg(1, "Starting in client mode");
  do_client();
}

unlink($g_conf_pidfile);

