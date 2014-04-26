#!/usr/local/bin/perl -w


use lib 'lib';
use Instance;
my $g_bin_path = "perl -I ../lib ../src/rstream.pl";

my $g_working_dir;
chomp($g_working_dir =`pwd`);



#$i->start();

use Data::Dumper;

#print Dumper($i);

#$i->stop();

sub compare_fs {
  my ($dir1, $dir2) = @_;

  print "Comparing file sets $dir1 and $dir2\n";
  my $ret = system("./compare_fs.pl $dir1 $dir2");
  if($ret == 0) {
    print "Identical\n";
    return(1);
  } else {
    print "Differ\n";
    return(0);
  }
}

sub test_1 {
  print "running test1\n";


  my $server = Instance->new(binpath => $g_bin_path,
                              server => 1, 
	   	                port => 4096, 
		  	         dir => "$g_working_dir/datasets/1" );
 
  my $client_path = "$g_working_dir/tmp/1";
  system("mkdir", "-p", $client_path);

  my $client = Instance->new(binpath => $g_bin_path,
                              server => 0, 
		  	         dir => "$g_working_dir/tmp/1",
			        port => 4096,
                             remotes => [ 'localhost' ] );
  $server->start(); sleep(1);
  $client->start(); 
  sleep(3);
  $server->stop();
  $client->stop();


  return(compare_fs("datasets/1", "tmp/1/localhost"));
}


my $g_tests = {
  "test_1" => { fp => \&test_1, desc => "Basic test to verify file replication" }
};




my $num_fails = 0;
for my $t (keys %{$g_tests}) {
  my $ret = $g_tests->{$t}{fp}();
  if(!$ret) {
    print "FAIL\t:" . $t . " - " . $g_tests->{$t}{desc} . "\n";
    $num_fails++;
  } else {
    print "OK\t:" . $t . " - " . $g_tests->{$t}{desc} . "\n";
  }
}
if($num_fails) {

  print "$num_fails failures\n";
  exit(1);
} else {
  print "All tests succeeded\n";
}


