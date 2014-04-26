package Instance;
use strict;



=head1 NAME

Instance.pm - Abstract process management for rstream testing


=cut

# class data

my $g_num_instances = 0;

=head2 new(  args  )

  args is a list of optional list with keys: 

  server   => 0 | 1         # run as server or client
  port     => 5201          # port to listen on when in server mode
  dir      => /var/logs     # path to share
  id       => sring         # arbitrary id to scope filenames
  remotes  => [ remote_serverlist ]  # remote hosts to connect to
  binpath  => "../src/rstream.pl" # path to rstream


=cut


sub new {
  my $class = shift;

  my $self = { verbose => 1, 
               id      => $g_num_instances,
	       port    => 4096,
	       server  => 1,
	       dir     => 'data',
	       remotes => [ 'localhost' ],
	       binpath => '../src/rstream.pl'

  };
  $g_num_instances++;

  my %args = @_;

  while (my ($key, $value) = each %args) {
    if (exists $self->{$key}) {
      $self->{$key} = $value;
    }
  }

  $self->{pidfile} = "/tmp/rstream_" . $self->{id} . ".pid";
  $self->{logfile} = "/tmp/rstream_" . $self->{id} . ".log";
  $self->{started}  = 0;

  bless($self, $class);
  return($self);
}

sub dir {
  my $self = shift;

  if(scalar(@_)) {
    $self->{dir} = shift @_;
  };
  return($self->{dir});
}

sub get_cmdline_args {
  my $self = shift;

  my $cmdline = '';

  $cmdline .= " -d " . "'". $self->{dir} . "'";
  $cmdline .= " -p " . "'". $self->{pidfile} . "'";
  $cmdline .= " -P " . $self->{port};

  if($self->{server}) {
    $cmdline .= " -l";
  } else {
    for my $s (@{$self->{remotes}}) {
      $cmdline .= " $s";
    }
  }
  return($cmdline);

}

sub log {
  my $self = shift;
  my $msg  = shift;
  my $exit = shift;
  return(0) if !$self->{verbose};

  my $str = time();
  if($self->{server}) {
    $str .= " server";
  } else {
    $str.= " client";
  }
  $str .= " " . $self->{id} .": " . $msg . "\n";
  print $str;
}

sub fail {
  my $self = shift;
  $self->log(@_);
  die "That was fatal...";
}


sub start {
  my $self = shift;

  my $cmdline = $self->{binpath} . " " .  $self->get_cmdline_args();
  unlink($self->{pidfile});
  $self->log("Starting new instance: $cmdline");
  system($cmdline) == 0 or $self->fail('Failed to fork');
  sleep(1);
  if(! -s $self->{pidfile}) {
    $self->fail("Failed to start new instance");
  }

  $self->{started}  = 1;
  $self->log("Success");
  return(1);
}

sub stop {
  my $self = shift;

  if(!$self->{started}) {
    $self->log("Not started");
    return(0);
  }

  if(!-s $self->{pidfile}) {
    $self->log("Cannot stop instance: missing pidfile");
    return(0);
  }

  my $tmp = "cat " . $self->{pidfile};
  my $pid = `$tmp`;
  system("kill", $pid);

  $self->log("Killing pid $pid");
  $self->{started} = 0;
}

sub DESTROY {
  my $self = shift;

  if($self->{started}) {
    $self->stop();
  }
}



1;
