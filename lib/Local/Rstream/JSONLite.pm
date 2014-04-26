
# Copyright 2014, Yahoo! Inc.
# This program is free software. You may copy or redistribute it under the same terms as Perl itself. 
# Please see the LICENSE file included with this project for the terms of the Artistic License under which this project is licensed.

package Local::Rstream::JSONLite;


require Exporter;
@ISA = qw(Exporter);

@EXPORT = qw(encode_hash_to_json encode_to_json decode_hash_from_json);

# todo:
# deal with escaped quotes in strings
# change for (keys %h) to while (each %h)

# JSON EBNF (or, my guess at it)
# 
# ELEMENT  ::= SCALAR | ARRAY | HASH
# KEYPAR   ::= '"' WORD '"' ':' ELEMENT
# HASH     ::= '{' KEYPAIR { "," KEYPAIR } '}'
# ARRAY    ::= '[' ( ELEMENT ) { "," ELEMENT ']'
# SCALAR   ::= '"' WORD '"' 


sub do_error {
  my $msg = shift @_;
  print "JSONLite: $msg\n";
  return(undef);
}

sub encode_to_json {
  my $ref = shift @_;  # reference to the data
  my $data = '';

  if(!ref($ref)) { #scalar
    $data .= "\"$$v\"";
  } elsif (ref($ref) eq "HASH") {
    $data .= encode_hash_to_json($ref);
  } elsif (ref($ref) eq "ARRAY") {
    $data .= encode_array_to_json($ref);
  }
  return($data);
}


sub encode_array_to_json {
  my $arr = shift @_;  # reference to the array
  my $first = 1;
  
  my $data = '[';
  for my $v (@$arr) {
    if($first) { $first = 0; } else { $data .= ","; }

    if(!ref($v)) { #scalar
	$data .= "\"$v\"";
    } elsif (ref($v) eq "HASH") {
      $data .= encode_hash_to_json($v);
    } elsif (ref($v) eq "ARRAY") {
      $data .= encode_array_to_json($v);
    }
  }
  $data .= "]";
  return($data);
}


sub encode_hash_to_json {
  my $hash = shift @_;  # reference to the hash
  my $first = 1;
  
  my $data = '{';
  for my $k (keys %$hash) {
    if($first) { $first = 0; } else { $data .= ","; }
    $data .= "\"$k\":";
    my $v = $hash->{$k};
    if(!ref($v)) { #scalar
      $data .= "\"$v\"";
    } elsif (ref($v) eq "HASH") {
      $data .= encode_hash_to_json($v);
    } elsif (ref($v) eq "ARRAY") {
      $data .= encode_array_to_json($v);
    }
  }
  $data .= "}";
  return($data);
}

sub scan_quoted_word {
  my $str_ref  = shift @_; # ref to input string
  my $pos_ref = shift @_;  # contains ref to the current scanning pos
  my $curpos = $$pos_ref;
  my $len = length($$str_ref);
# we're at the first ", find the second one
# todo: fix this for escaped quotes
  my $qpos = index($$str_ref, '"', $curpos + 1);
  return(undef) if($qpos == -1);
  my $word = substr($$str_ref, $curpos + 1, $qpos - ($curpos + 1));
  $$pos_ref = $qpos + 1;  # set position to after 2nd "
  return($word);          # return the word we extracted
}

# get the first non-whitespace character
sub scan_next_word {
  my $str_ref = shift @_; # ref to input string
  my $pos_ref = shift @_;  # contains ref to the current scaning pos
  my $curpos = $$pos_ref;
  my $len = length($$str_ref);

  while($curpos < $len) {
    my $char = substr($$str_ref, $curpos, 1);
    if((ord($char) > 32) && (ord($char) < 127)) {
      $$pos_ref = $curpos;
      return($char);
    }
    $curpos++;
  }
  $$pos_ref = $curpos;
  return(undef);
}

# scan for the next quote, checking for escaped quotes
sub scan_next_quote {
  my $str_ref = shift @_;  # ref to input string
  my $pos_ref = shift @_;  # contains ref to the current scaning pos

  while(my $char = scan_next_word($str_ref, $pos_ref)) {
    if($char eq '"') {
      if($$pos_ref == 0) { return(1); }  # can't be escaped at start
      if(substr($$str_ref, $$pos_ref - 1) ne "\\") { return(1); }
      $$pos_ref = $$pos_ref + 1;
    }
  }
  return(undef);
}

sub scan_hash {
  my $str_ref = shift @_;  # ref to input string
  my $pos_ref = shift @_;   # ref to current position in string
  local ($token, $key, $valref);

  local $hash = {};

  while(1) {
    $token = scan_next_word($str_ref, $pos_ref);
    last if($token eq "}" ); # check for empty list
    if(!defined($token) or $token ne '"') {  return(do_error("scan_hash: failed to find starting quote")); } 
    $key = scan_quoted_word($str_ref, $pos_ref);
    if(!defined($key)) { return(do_error("scan_hash: could not extract key")); } # dbg

    $token = scan_next_word($str_ref, $pos_ref);
    if(!defined($token) or $token ne ':') { return(do_error("scan_hash: failed to find :")); }
    $$pos_ref++;

    $valref = scan_element($str_ref, $pos_ref);
    if(!defined($valref)) { return(do_error("scan_hash: failed to find hash value")); }

    # if we found a scalar, store the value, not a reference
    if(ref($valref) eq "SCALAR") { #scalar
      $hash->{$key} = $$valref;
    } else {
      $hash->{$key} = $valref;
    }
    $token = scan_next_word($str_ref, $pos_ref);
    $$pos_ref++;
    if(!defined($token) || ($token ne "," && $token ne "}" )) { 
      return(do_error("scan_hash: block unterminated"));
    }

    last if($token eq "}" );
  }
  return($hash);
}


sub scan_array {
  my $str_ref = shift @_;  # ref to input string
  my $pos_ref = shift @_;   # ref to current position in string
  local ($token, $key, $valref);

  local $arr = [];

  while(1) {
    $valref = scan_element($str_ref, $pos_ref);
    if(!$valref) { return(do_error("scan_array: failed to find element")); }

    # if we found a scalar, store the value, not a reference
    if(ref($valref) eq "SCALAR") { #scalar
      push @$arr, $$valref;
    } else {
      push @$arr, $valref;
    }
    $token = scan_next_word($str_ref, $pos_ref);
    $$pos_ref++;
    if(!$token || ($token ne "," && $token ne "]" )) { 
      return(do_error("scan_array: block unterminated"));
    }
    last if($token eq "]" );
  }
  return($arr);
}

sub scan_element {
  my $str_ref = shift @_;  # ref to input string
  my $pos_ref = shift @_;   # ref to current position in string

  local ($curpos, $token, $val);

  $val = ''; $token = '';
  $token = scan_next_word($str_ref, $pos_ref);
  if(!$token) { return(do_error("Failed to scan element")); }

  if($token eq '"') {  # scalar value
    $val = scan_quoted_word($str_ref, $pos_ref);
    if(!defined($val)) { return(do_error("Missing value")); }
    return(\$val);
  } elsif($token eq '{') {  # hash
    $$pos_ref++;
    return(scan_hash($str_ref, $pos_ref));
  } elsif($token eq '[') {  # array
    $$pos_ref++;
    return(scan_array($str_ref, $pos_ref));
  }
  return(undef); # nothing interesting found
}


sub decode_hash_from_json { 
  my $input = shift @_;
  my $pos = 0;
  my %hash;

  return(scan_element(\$input, \$pos));
}

1;

