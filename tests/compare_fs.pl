#!/usr/local/bin/perl -w


use Getopt::Long           qw(:config no_ignore_case);
use Fcntl;
use Digest::SHA;
use lib '../lib';
use Local::Rstream::Dirscan;

use constant FILE_BLOCK_SIZE            => 8192;

my $g_conf_verbose = 0;

my $g_helpmsg = qq(
    Compare the contents of two directories

    Usage: 

      compare_fs.pl [options] dir1 dir2

    
     Options:
      -h --help           : display this help
      -v --verbose        : increase verbosity of log messages 
   );



sub calc_checksum {
  my $filename  = shift @_;    # name and path of file
  my $offset = undef;
  if(scalar(@_)) { 
    $offset    = shift @_;    # calc hash over first $offset bytes
  } else {
    $offset = (stat($filename))[7];
  }
  
  my $sha = Digest::SHA->new(1);

  my $FD;


  my ($ret, $blocksize, $bytes_read, $block, $filedata);

  $bytes_read = 0;

  if(!sysopen($FD, $filename, O_RDONLY)) {
    die("calc_checksum: Failed to open $filename");
    return(undef);
  }

  while($bytes_read < $offset) {
    $blocksize = $offset - $bytes_read;
    $blocksize = FILE_BLOCK_SIZE if($blocksize > FILE_BLOCK_SIZE);
    $ret = sysread($FD, $block, $blocksize);
    if(!$ret) {
      close($FD);
      die("calc_checksum: Failed to read $blocksize bytes at offset $bytes_read on file $filename");
      return(undef);
    }
    $bytes_read += $ret;
    $sha->add($block);
  }
  close($FD);
  my $hash = $sha->hexdigest();
  return($hash);
}

sub compare_states {
  my $s1 = shift;
  my $s2 = shift;
  my $diffs = '';

  for my $f (keys %{$s1->{state}}) {
    if(!exists($s2->{state}{$f})) {
      $diffs .=  $f . ": missing from " . $s2->{path} . "\n";
      next;
    }
    if($s1->{state}{$f}{size} != $s2->{state}{$f}{size}) {
      $diffs .= "$f: size differs\n";
      next;
    }
    if($s1->{state}{$f}{hash} ne  $s2->{state}{$f}{hash}) {
      $diffs .= "$f: hash differs\n";
      next;
    }
  }
  return($diffs);
};


my $help = 0;
if(!GetOptions(
      "verbose|v+"    => \$g_conf_verbose,
      "help|h"        => \$help
      ) || scalar(@ARGV) != 2 || $help) {
# todo: print help text
  print "$g_helpmsg\n";
  exit 1;
}

my @g_dir;

$g_dir[0] = {path => shift @ARGV };
$g_dir[1] = {path => shift @ARGV };

my $sha = Digest::SHA->new(1);

# save current dir
my $topdir = `pwd`; chomp($topdir);


use Data::Dumper;

foreach my $i qw(0 1) {
  
  my $dir = $g_dir[$i];


  chdir $topdir;

  if(! -d $dir->{path}) {
    die $dir->{path} . ": no such dir";
  }

  chdir $dir->{path};


  $dir->{ds} = Local::Rstream::Dirscan->new(".");
  $dir->{ds}->scan();
  $dir->{state} = {};
  foreach my $f ( $dir->{ds}->new_files() ) {
    $dir->{state}{$f} = {
      size => (stat($f))[7],
      hash => calc_checksum($f)
    };
  }
  
}



my $report = '';
$report .= compare_states(@g_dir);
$report .= compare_states($g_dir[1], $g_dir[0]);

if(!$report) {
  print "No differences\n";
  exit(0);
}
else {
  print $report . "\n";
  exit(1);
}



