# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself.
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

package Local::Rstream::Netlib;


use Socket;
use Fcntl;
require Exporter;
@ISA = qw(Exporter);

@EXPORT = qw(create_server_sock accept_client set_fbits connect_tcp_sock);


sub create_server_sock { 
  my ($protoname, $port)  = @_;
  my $proto = getprotobyname($protoname);
  local *SERVER;

  socket(SERVER, PF_INET, SOCK_STREAM, $proto)        || die "socket: $!";
  setsockopt(SERVER, SOL_SOCKET, SO_REUSEADDR,
				 pack("l", 1))        || die "setsockopt: $!";
  bind(SERVER, sockaddr_in($port, INADDR_ANY))        || die "bind: $!";
  listen(SERVER,SOMAXCONN)                            || die "listen: $!";

  return(*SERVER);
}


sub accept_client {
  my $server = shift @_;      # server socket
  my $client_info = undef;
  if(scalar(@_) > 0) {
    $client_info = shift;
  }
  local *CLIENT;

  my $paddr = accept(CLIENT, $server);
  if(!$paddr) { next; }
  my($port,$iaddr) = sockaddr_in($paddr);
  my $name = gethostbyaddr($iaddr,AF_INET);

  fcntl(CLIENT, F_SETFL, O_NONBLOCK) or print "accept_client: Cannot set client socket to nonblocking\n";

  $client_info->{ip}   = inet_ntoa($iaddr);
  $client_info->{port} = $port;
  return(*CLIENT);
}

sub connect_tcp_sock {
  my $hostname = shift @_;    # hostname
  my $port     = shift @_;    # port num or text name

  local *SOCK;

  if ($port =~ /\D/) { $port = getservbyname($port, 'tcp') }

  my $iaddr   = inet_aton($hostname)  || return(undef);
  my $paddr   = sockaddr_in($port, $iaddr);

  $proto      = getprotobyname('tcp');
  if(!socket(SOCK, PF_INET, SOCK_STREAM, $proto)) {
    print "connect_tcp_sock: Failed to create socket: $!\n";
    return(undef);
  }
  if(!connect(SOCK, $paddr)) {
    print "connect_tcp_sock: Failed to connect to [$hostname:$port] - $!\n";
    return(undef);
  }
  fcntl(SOCK, F_SETFL, O_NONBLOCK) or print "accept_client: Cannot set client socket to nonblocking\n";
  return(*SOCK);
}


sub set_fbits
{
  my ($f, $rin);

  $rin = '';
  foreach $f (@_) { vec($rin, $f, 1) = 1; }
  return $rin;
}


__END__

=head1 NAME

Local::Rstream::Netlib - A module with basic functions for accepting or connecting to TCP sockets.

=head1 SYNOPSIS


#!/usr/bin/perl -w


# immplement a server accepting and reading from multiple client connections.

use Local::Rstream::Netlib;

my %g_clients = ();
my $g_port    = 8080;


$server = create_server_sock("tcp", $g_port) or die "Failed to create server socket";
print "Listening on port $g_port\n";

while(1) {

  # initialise file handle bitsets
  # always be willing to read from server socket (accepts) and client sockets 
  # only attempt to write if there is data pending to be sent
  my $rin = set_fbits(fileno($server), keys %g_clients);
  my $rout = '';

  my $ret = select($rout=$rin, undef, undef, 2);

  if($ret > 0) {
    # check for incoming connection
    if(vec($rout, fileno($server), 1)) {
      my %info = ();
      my $client =  accept_client($server, \%info);
      if(!$client) { die "Failed to accept incoming connection"; }
      $g_clients{fileno($client)} = { 'fh' => $client };
      print "Got connection from [$info{ip}]\n";
    }
    # check each of the client sockets for reading
    for my $fd (keys %g_clients) {
      if(vec($rout, $fd, 1)) {
	local *FH;
	*FH = $g_clients{$fd}->{fh};
	print "$fd wants to talk\n";
	while($line = <FH>) {
	  print "client: [$line]\n";
	}
      }
    }
  }
}


=head1 FUNCTIONS

=over

=item create_server_sock()

  Sets up a server socket for listening.

  Args: 
    protoname     - "tcp" or "udp"
    port          - numerical port number


=item accept_client()

  Pops a socket off the listen queue.

  Args:
    server        - the server sock fd
    info          - ref to hash, will set 'ip' and 'port' values on return

  Returns the client socket.


=item connect_tcp_sock()

  Connects to a remote socket.

  Args:
    host          - hostname
    port          - port or service



=item set_fbits()

  Returns a bitset with the file descriptors set, which can be passed to select()

  Args: a list of file descriptors.
     
=back
=cut
