# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself. 
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

package Local::Rstream::Filescan;
  
=head1 NAME

Local::Rstream::Filescan - read incremental changes to multiple files.

=head1 SYNOPSIS

  use Local::Rstream::Filescan;

  sub new_data {
    my $filename = shift @_;
    my $data     = shift @_;
    my $size     = shift @_;
    print "Got $size bytes new data for [$filename]:\n$data\n";
  }

  $fs = Local::Rstream::Filescan->new(
		      locations     => [ "./logs" , "/var/log/app" ], # directories
		      include_masks => [ '*.log' ], 
		      callback      => \&new_data
		      );

  my $next_scan = 0;
  while(1) {
    if($next_scan < time()) {
      $next_scan = time() + 1;
    }
    if(!$fs->scan()) {
      # callback will be called for each block of data read, like this:
      # callback(filename, data_block, length);

      sleep(1);
    }
  }

=head1 DESCRIPTION


Recursively scan a directory for files with matching file names, read them, and monitor for incremental changes.
Offsets are saved to disk so reads can resume at last position between restarts.


=cut

use NDBM_File;
use Fcntl;
use Local::Rstream::Dirscan;
use Local::Rstream::IOBuffer;

=pod

  File changes are detected like this:
    - New/deleted files: detected by comparing successive scans of the directories
    - File appends     : attempting to read past end of file
    - File truncates   : refreshing stat info and comparing before and after file sizes
    - file replace     : not implemented

=cut


=head1 METHODS

=over

=item new()

  Contructer. Optional arguments in hash key/value pairs:

  Arg                   Default         Description
  offset_filename       .filescan       Name of NDBM where file offsets are saved       
  locations             .               Ref to array of directory locations to monitor
  exclude_masks         all             Ref to array of filename masks to select for inclusion
  include_masks         all             Ref to array of filename masks to select for inclusion
  callback		none		Ref to user function
  dir_scan_interval	5		Interval in seconds between scans for new or deleted files
  stat_interval		6		Interval in seconds between scans for file truncates
  line_buffered		true		Deliver only complete lines to user



=cut


sub new {
  my ($class, %options) = @_;
  my $self  = {};
  $self->{offset_filename}   = ".filescan";
  $self->{locations}         = [ "." ];
  $self->{exclude_masks}     = [];
  $self->{include_masks}     = [];
  $self->{dir_scan_interval} = 2; # num seconds between scanning for new/deleted files
  $self->{stat_interval}     = 5;    # num seconds between checking for truncates
  $self->{line_buffered}     = 1;     
  $self->{debug}             = 0;
  $self->{callback}          = undef;

  while((my $k, $v) = each(%options)) {
    $self->{$k} = $v;
  }

  $self->{files} = {};    # table containing file records, indexed by filename.
  $self->{offsets} = {};  # we need to keep the offsets seperately because ndbm can only store 
                          # simple data structures. Indexed by filename.
  $self->{next_dir_scan} = 0; # timestamp of next scan for new/removed files

  $self->{remove_queue} = {};

  tie(%{$self->{offsets}}, 'NDBM_File', $self->{offset_filename},
			      O_RDWR | O_CREAT, 0666) or
	die "Failed to tie to " . $self->{offset_filename} . " - $!";
  $self->{ds} = Local::Rstream::Dirscan->new();
  $self->{ds}->set_include_masks(@{$self->{include_masks}});
  $self->{ds}->set_exclude_masks(@{$self->{exclude_masks}});

  locations($self, @{$self->{locations}});
  bless($self, $class);
}


=item location(@directories)

  Set the list of directories to monitor. 
  Pass empty args to return the set of directories that are monitored.

=cut

sub locations {
  my $self   = shift @_;
  if(scalar(@_) > 0) {
    my $locations = shift @_;
    $self->{ds}->set_locations($locations);
  } else {
    return(@{$self->{locations}});
  }
}

=item set_callback(ref to function)

  Set the function to call when new data has arrived.

  Arguments passed to the callback:
  filename, data, length

=cut

sub set_callback {
  my $self   = shift @_;
  my $fn     = shift @_;   # scalar ref to function

  $self->{callback} = $fn;
}



=item line_buffered()

  Set to true if input is line buffered and only complete lines must be returned 
  to the user. Note a block of data passed to the user may contain multiple lines.

=cut

sub line_buffered {
  my $self   = shift @_;
  if(scalar(@_) > 1) {
    my $val = shift @_;
    $self->{line_buffered} = $val;
  } else {
    return($self->{line_buffered});
  }
}

=item scan()

  Scan the files for data changes, passing any data read back to the user via the callback function. Returns true if any data was read.

=cut

sub scan {
  my $self = shift;

  my $now = time();
  my $data_read = 0;
  if($now > $self->{next_dir_scan}) {
    $self->check_dir_changes();
    $self->{next_dir_scan} = $now + $self->{dir_scan_interval};
  }

  $self->process_removed_file_queue();

  while(my ($fname, $f) = each(%{$self->{files}})) {

    next if $self->is_flagged_for_removal($fname);

    # open file
    if(!defined($f->{fh})) {
      if($self->{debug}) { print "scan: opening $fname\n"; }
      next if(!$self->open_file($fname));
    }

    # check for truncate / restart
    if($f->{next_stat} < $now) {
      $f->{next_stat} = $now + $self->{stat_interval};
      my ($nlinks, $size) = (stat($f->{fh}))[3,7];

      if(!defined($size) || !$nlinks) {   # stat failed
#$self->flag_for_removal($fname);
	$self->close_file($fname);
	next;
      }
      if($size < ${$f->{offset}}) {
	if($self->{debug}) { print "scan: $fname restarted\n" ; }
	${$f->{offset}} = 0;
	$self->close_file($fname);
	next;
      }
      # if stat failed because file has been deleted, it willl be processed in
      # check_dir_changes()
    }

    my $block = "";

    # do not attempt to read if buffer is full
    next if($f->{buf}->len() > 8192);
    $ret = sysread($f->{fh}, $block, 4096);
    if(!defined($ret)) {   # read failure- close and reopen later
      if($self->{debug}) { print "scan: read failed on $fname"; }
      $self->close_file($fname);
      next;
    }
    next if(!$ret); # eof - nothing to do
    $data_read++;


    # only update the offset once the data has been cleared the buffer
    #${$f->{offset}} = sysseek($f->{fh}, 0, Fcntl::SEEK_CUR);
    $f->{buf}->add($block, $ret);
    if($self->{debug}) { print "$fname: read $ret bytes\n"; }
  }
  $self->dispatch_data();
  return($data_read);
}



#--------- Private functions ---------------------------------#

sub strip_filename {         # remove filename from path
  my $path_and_filename = shift @_;

  my $pos = rindex($path_and_filename, "/");
  if($pos == -1)  { return ($path_and_filename); }
  return(substr($path_and_filename, 0, $pos));
}

sub init_saved_status {
  my $self = shift;
  while(my ($f, $o) = each(%{$self->{offsets}})) {
    $self->add_file_record($f, $o) if($o > -1);
  }
}

sub open_file {
  my $self = shift;
  my $filename = shift;
  my $fh = undef;

  $f = $self->{files}{$filename}; return(0) if(!$f);


  if($self->{debug}) { print "open_file: $filename at offset ". ${$f->{offset}} . "\n"; }

  my ($size, $mtime) = (stat($filename))[7,9];
  if(!defined($size)) {  # can't stat the file
    return(0);
  }

  if(!open($fh, $filename)) {
    if($self->{debug}) { print("Filescan: Failed to open file $filename\n"); }
    return(0);
  }
  $f->{fh} = $fh;

  if($size < ${$f->{offset}}) {
    # file has been truncated since we last ran
    if($self->{debug}) { print "open_file: $filename truncated, restarting\n"; }
    ${$f->{offset}} = 0;
  }

  # seek to saved position
  if(${$f->{offset}} > 0) {
    if($self->{debug}) { print "restoring $filename from offset [" . ${$f->{offset}} . "]\n"; }

    my $pos = sysseek($f->{fh}, ${$f->{offset}}, 0);
    if(!defined($pos)) {
      ${$f->{offset}} = 0;
      $self->close($filename);
      $f->{fh} = undef;
      return(0);
    }
  }
  return(1);
}

sub close_file {
  my $self = shift;
  my $filename = shift;

  if($self->{debug}) { print "close_file: $filename\n"; }
  $f = $self->{files}{$filename}; return(0) if(!$f);

  close($f->{fh});
  $f->{fh} = undef;
  return(1);
}

# Setup the data structures for a file
sub add_file_record {
  my $self = shift;
  my $filename = shift;
  my $offset = 0;
  $offset = shift if(scalar(@_));
  my $offset_ref;

  $self->{files}{$filename} = {};

  if(!defined($self->{offsets}{$filename})) {
    $self->{offsets}{$filename} = $offset;
  }
  $offset_ref = \$self->{offsets}{$filename};
  if($$offset_ref == -1) {  # if file was previously deleted
    $$offset_ref = 0;
  }

  $self->{files}{$filename}{offset} = $offset_ref;
  $self->{files}{$filename}{fh} = undef;
  $self->{files}{$filename}{buf} = Local::Rstream::IOBuffer->new();
  $self->{files}{$filename}{last_read} = 0;          
  $self->{files}{$filename}{next_stat} = time() + 
    $self->{stat_interval};
  if($self->{debug}) { print "Added [$filename]\n"; }
}


sub is_flagged_for_removal {
  my $self = shift;
  my $filename = shift;
  return(exists($self->{remove_queue}{$filename}));
}

sub flag_for_removal {
  my $self = shift;
  my $filename = shift;
  $self->{remove_queue}{$filename} = 1;
}

sub process_removed_file_queue {
  my $self = shift;

  my @list = keys %{$self->{remove_queue}};

  for my $fname (@list) {
    # do not remove if there is data queued in buffer
    next if($self->{files}{$fname}{buf}->len());


    # can't delete keys from a ndbm file
    $self->{offsets}{$fname} = -1;
    $self->{files}{$fname}{buf} = undef;
    delete($self->{files}{$fname});

    delete($self->{remove_queue}{$fname});
    if($self->{debug}) { print "removed data for: $fname\n"; }
  }
}

sub check_dir_changes {
  my $self = shift;

  $self->{ds}->scan();

  for my $f ($self->{ds}->new_files()) {
    if(!exists($self->{files}{$f})) {
      $self->add_file_record($f);
    }
  }
  for my $f ($self->{ds}->removed_files()) {
    if(exists($self->{files}{$f})) {
      $self->flag_for_removal($f);
    }
  }
}


sub dispatch_data {
  my $self   = shift;

  return if(!$self->{callback});
  while(my ($fname, $f) = each(%{$self->{files}})) {
    my $datalen = $f->{buf}->len();
    next if(!$datalen);

    if($self->{line_buffered}) {
      my $p = $f->{buf}->peek();
      my $end = rindex($p, "\n");
      next if ($end == -1);
      $datalen = $end + 1;
    }

    $d = $f->{buf}->get($datalen);
    &{$self->{callback}}($fname, $d, $datalen);
    if($self->{debug}) { print "dispatch_data: $fname\n"; }
    ${$f->{offset}}+=$datalen;
  }

}
1;

__END__

=back

=head1 LIMITATION

No checksumming or hashing is done on files. If a file is replaced by a bigger file, only the data from the difference in size is returned.

scan() will block until all files are read or checked for data, which could be a long time for large file sets.

=cut




