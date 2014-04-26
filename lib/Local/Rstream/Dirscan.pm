# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself. 
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

package Local::Rstream::Dirscan;
  
=head1 NAME

Local::Rstream::Dirscan - monitoring directories for file creation and deletion.

=cut

use Fcntl;

=head1 SYNOPSIS

  use Local::Rstream::Dirscan;

  my $ds = Local::Rstream::Dirscan->new("./logs");
  $ds->set_include_masks("^access$", "^error$")
  $ds->set_exclude_masks(qw(\.gz$));

  # get a once off list of files
  $ds->scan();
  for my $f ($ds->files()) {
    print "[$f]\n";
  }

  # monitor for changes
  my $next_scan = 0;
  while(1) {
    if($next_scan < time()) {
      $next_scan = time() + 3;
      $ds->scan();

      foreach my $v ($ds->new_files()) {
	print "New: [$v]\n";
      }

      foreach my $v ($ds->removed_files()) {
	print "Removed: [$v]\n";
      }
    }
  }

=cut

=head1 METHODS

=over

=item new(@directories)

Initialise the object with the specified directories to scan.

=cut

sub new {
  my $class = shift;
  my $self  = {};
  $self->{files} = {};          # the full set of files found in last scan
  $self->{new_files} = {};      # files detected to be newly added in last scan
  $self->{removed_files} = {};  # files removed since the previous scan
  $self->{include_masks} = [];  # filename masks to include files
  $self->{exclude_masks} = [];  # filename masks to exclude files
  $self->{recurse}       = 1;   # by default recurse
  set_locations($self, @_);
  bless($self, $class);
}

=item set_locations(@directories)

Set the list of directories to monitor.

=cut


sub set_locations {
  my $self = shift;
  my %tmphash;

  $self->{dirs} = undef;
  $self->{dirs} = [];
  while(my $d = shift @_) {
    $tmphash{$d} = 1;             # use hash to squash duplicates
  }
  $self->{dirs} = [ keys %tmphash ];
}

=item get_locations()

  Returns list of directories being monitored

=cut



sub get_locations {
  my $self = shift;
  return(@{$self->{dirs}});
}

=item set_include_masks(@regex_patterns)

  Set a list of regex patterns against which to test file names for inclusion into the set.

=cut

sub set_include_masks {
  my $self = shift;

  $self->{include_masks} = undef;
  $self->{include_masks} = [];
  while(my $d = shift @_) {
    push(@{$self->{include_masks}}, $d);
  }
}

=item get_include_masks()

  Returns the list of filename inclusion patterns.

=cut

sub get_include_masks {
  my $self = shift;
  return(@{$self->{include_masks}});
}

=item set_exclude_masks(@regex_patterns)

  Set the list of filename patterns to ignore. Exclusions are processed before inclusions.

=cut

sub set_exclude_masks {
  my $self = shift;

  $self->{exclude_masks} = undef;
  $self->{exclude_masks} = [];
  while(my $d = shift @_) {
    push(@{$self->{exclude_masks}}, $d);
  }
}

=item get_exclude_masks()

  Returns a list of filename exclusion masks.

=cut
sub get_exclude_masks {
  my $self = shift;
  return(@{$self->{exclude_masks}});
}

=item get_locations()

  Returns a list of all the files scanned.

=cut

sub files {
  my $self = shift;
  if(wantarray()) { return(keys %{$self->{files}}); }
  return($self->{files});
}

=item new_files()

  Returns a list of newly added files since previous scan.

=cut
sub new_files {
  my $self = shift;
  if(wantarray()) { return(keys %{$self->{new_files}}); }
  return($self->{new_files});
}

=item removed_files()

  Returns the list of files that were removed since the previous scan.

=cut
sub removed_files {
  my $self = shift;
  if(wantarray()) { return(keys %{$self->{removed_files}}); }
  return($self->{removed_files});
}

=item set_recurse(scalar)

  Controls whether sub directories are scanned recursively.
  Set to true (default) or false.

=cut

sub set_recurse {
  my $self = shift;
  $self->{recurse} = shift;
}

sub scan_dir {  
    my $self = shift;         # object
    my $path = shift;         # path to start search
    my $new_files = shift;    # ref to hash to return list of new files
    my $recurse = shift;      # flag to control recursion

    my @dirs = ();
    my $file;
    my $fullname;
    local *DIR;
    opendir(DIR, $path) or return(undef);

    while($file = readdir(DIR)) {
      # check file against eclude patterns
      next if($file eq "." or $file eq "..");

      $fullname = "$path/$file";
      if(-d $fullname) {
	push(@dirs, "$fullname");
	next;
      }

      my $exclude = 0;
      foreach my $pat (@{$self->{exclude_masks}}) {
	if($file =~ /$pat/) {
	  $exclude = 1;
	  last;
	}
      }
      next if($exclude);

      if($self->{include_masks} && scalar(@{$self->{include_masks}}) > 0) {
	$exclude = 1;
	foreach my $pat (@{$self->{include_masks}}) {
	  if($file =~ /$pat/) {
	    $exclude = 0;
	    last;
	  }
	}
	next if($exclude);
      }

      $new_files->{$fullname} = 1;
    }
    closedir(DIR);
    # recurse dirs now
    if($recurse) {
      while($file = shift(@dirs))
      {
	$self->scan_dir("$file", $new_files, $recurse);
      }
    }
    return(undef);
}

=item scan()
  
  Scan the directories for changes. 
  Use files(), new_files(), and removed_files() to inspect the results.

=back

=cut

sub scan {
  my $self = shift;
  my %curset;
  my $changed = 0;

  $self->{new_files} = undef;
  $self->{new_files} = {};
  $self->{removed_files} = undef;
  $self->{removed_files} = {};

  for my $path (@{$self->{dirs}}) {
    $self->scan_dir($path, \%curset, $self->{recurse});
  }

  # get newly added files
  while(my $f = each(%curset)) {
    if(!exists($self->{files}{$f})) {
      $self->{new_files}{$f} = 1;
      $changed++;
    }
  }

  # get removed files
  while(my $f = each(%{$self->{files}})) {
    if(!exists($curset{$f})) {
      $self->{removed_files}{$f} = 1;
      $changed++;
    }
  }
  $self->{files} = undef;
  $self->{files} = \%curset;
  return($changed);
}

1;
