# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself.
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

package Local::Rstream::IOBuffer;


#------------------------------------------------------------------#
# Implement a FIFO buffer
#------------------------------------------------------------------#

sub new {
  my $class = shift;
  my $self = {};
  $self->{size} = 65536;  # only used for limit checking
  $self->{len} = 0;
  $self->{buf} = '';
  bless($self, $class);
}

sub size {
   my $self = shift;
   if (@_) { $self->{size} = shift }
   return $self->{size};
}

sub len {
   my $self = shift;
   return $self->{len};
}

# return how much space left in buffer
sub space { 
   my $self = shift;
   return($self->{size} - $self->{len});
}


sub add {
  $self = shift @_;
  my $data = shift @_;
  my $len;
  if(@_) {  $len = shift @_; } else { $len = length($data); }
  my $space = $self->{size} - $self->{len};
  if($len > $space) {
      printf "y7buffer: size exceeded ($len requested, $space available)\n";
      # don't return an error, just warn, because applications may find it difficult to recover from such a error
  }

  # surely this won't work... will it?
  $self->{buf} .= $data;
  $self->{len}+=$len;
  return($self->{len});
}

# args: length to shift
sub get {
  $self = shift @_;
  $len = shift @_;
# todo: use substr to extract data from front of buffer, and shrink buffer
  my $shifted_data;

  if($len > $self->{len}) { $len = $self->{len}; };
  return(undef) if !$len;
  $self->{len} -= $len;
  $shifted_data = substr($self->{buf}, 0, $len, '');
  return($shifted_data);
}

# return the start of the buffer but don't shift or pop anything off
sub peek {
  $self = shift @_;
  return($self->{buf});
}

# push back data to the front of the buffer
sub push {
  $self = shift @_;
  my $data = shift @_;
  my $len;
  if(@_) {  $len = shift @_; } else { $len = length($data); }
  $self->{buf} = $data . $self->{buf};
  $self->{len}+=$len;
  return($self->{len});
}

sub print {
  $self = shift @_;

  print("buf stats: len: " . $self->len() . \
    "data: [" . $self->peek() . "]" . \
    "\n");
}

1;


__END__

=head1 NAME

Local::Rstream::IOBuffer - A class implementing a FIFO buffer

=head1 SYNOPSIS

  use Local::Rstream::IOBuffer;

  my $buf = Local::Rsteam::IOBuffer->new();
  $buf->add("data");
  print $buf->get(4) . "\n";


=over

=item new()

  Create a new buffer object.

=item size()

  Returns or sets the max size of the buffer.

=item len()

  Returns the length of the data in the buffer.

=item space()

  Returns how much space is left in the buffer.

=item add()

  Adds data to the end of the buffer.

=item get()

  Removes the specified amount of data from the front of the buffer.

=item push()

  Pushes back the data to the beginning of the buffer.

=item peek()

  Return the start of the buffer but don't shift or pop anything off

=cut
