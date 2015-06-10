package OWD::ConsensusAnnotation;
@ISA = qw( OWD::Annotation );

use strict;
use List::MoreUtils;

my $valid_doctypes = {
	'cover'		=> 1,
	'blank'		=> 1,
	'diary'		=> 1,
	'other'		=> 1,
	'orders'	=> 1,
	'signals'	=> 1,
	'report'	=> 1,
};

sub new {
	my ($class, $_cluster, $_consensus_annotation) = @_;
	my $obj = bless {
		'_cluster'				=> $_cluster,
		'_annotation_data'		=> $_consensus_annotation,
	}, $class;
	
	# validate first - check for missing fields or invalid data that can't be fixed.
	if ($obj->_data_consistent()) {
		return $obj;
	}
	else {
		$obj->DESTROY();
		return undef;
	}
}

sub _data_consistent {
	# in addition to the consistency check on the individual annotations, the ConsensusAnnotation 
	# consistency check makes sure that we still have a meaningful annotation object after establishing
	# a consensus. If there was no consensus in a cluster, or not enough annotations in a cluster,
	# it's likely that the consensus annotation also won't be much use.
	my ($self) = @_;
	if ($self->{_annotation_data}{type} eq 'doctype') {
		if (List::MoreUtils::any {$self->{_annotation_data}{standardised_note} eq $_} keys %$valid_doctypes) {
			return 1;
		}
		return 0;
	}
	elsif ($self->{_annotation_data}{type} eq 'diaryDate'
			|| $self->{_annotation_data}{type} eq 'activity'
			|| $self->{_annotation_data}{type} eq 'domestic'
			|| $self->{_annotation_data}{type} eq 'orders'
			|| $self->{_annotation_data}{type} eq 'signals'
			|| $self->{_annotation_data}{type} eq 'reference'
			|| $self->{_annotation_data}{type} eq 'casualties'
			|| $self->{_annotation_data}{type} eq 'title'
			|| $self->{_annotation_data}{type} eq 'weather'
			|| $self->{_annotation_data}{type} eq 'unit'
			|| $self->{_annotation_data}{type} eq 'date'
			|| $self->{_annotation_data}{type} eq 'mapRef'
			|| $self->{_annotation_data}{type} eq 'gridRef'
			|| $self->{_annotation_data}{type} eq 'time'
			|| $self->{_annotation_data}{type} eq 'strength') {
		if (defined($self->{_annotation_data}{standardised_note})) {
			return 1;
		} 
		else {
			return 0;
		}
	}
	elsif ($self->{_annotation_data}{type} eq 'place') {
		if (defined($self->{_annotation_data}{standardised_note}) && $self->{_annotation_data}{standardised_note}{place} ne '') {
			return 1;
		} 
		else {
			return 0;
		}
	}
	elsif ($self->{_annotation_data}{type} eq 'person') {
		if (defined($self->{_annotation_data}{standardised_note}{surname})) {
			return 1;
		} 
		else {
			return 0;
		}
	}
	else {
		undef;
	}
}

sub has_disputed_data {
	my ($self) = @_;
	if (ref($self->{_annotation_data}{type}) eq 'ARRAY') {
		return 1;
	}
	else {
		if (ref($self->{_annotation_data}{standardised_note}) eq 'HASH') {
			foreach my $standardised_note_field (keys %{$self->{_annotation_data}{standardised_note}}) {
				if (ref($self->{_annotation_data}{standardised_note}{$standardised_note_field}) eq 'ARRAY') {
					return 1;
				}
			}
			return 0;
		}
		elsif (ref($self->{_annotation_data}{standardised_note}) eq 'ARRAY') {
			undef;
		}
		else {
			return 0;
		}
	}
}

sub resolve_disputes {
	my ($self) = @_;
	if (ref($self->{_annotation_data}{type}) eq 'ARRAY') {
		undef;
	}
	if (ref($self->{_annotation_data}{standardised_note}) eq 'HASH') {
		# a multi value note
		foreach my $standardised_note_field (keys %{$self->{_annotation_data}{standardised_note}}) {
			if (ref($self->{_annotation_data}{standardised_note}{$standardised_note_field}) eq 'ARRAY') {
				# disputed winning value
				if ($self->{_annotation_data}{type} eq 'place') {
					# if it's a PLACE annotation
					if ($standardised_note_field eq 'location') {
						if ($self->{_annotation_data}{coords}[0] < 12) {
							$self->{_annotation_data}{standardised_note}{$standardised_note_field} = 'true';
						}
						else {
							$self->{_annotation_data}{standardised_note}{$standardised_note_field} = 'false';
						}
					}
					elsif (		$standardised_note_field eq 'id'
							||	$standardised_note_field eq 'lat'
							|| 	$standardised_note_field eq 'long'
							|| 	$standardised_note_field eq 'name'
							|| 	$standardised_note_field eq 'placeOption') {
						# if this field is disputed, chances are we don't have enough annotations
						$self->{_annotation_data}{standardised_note}{$standardised_note_field} = undef;
					}
					else {
						# we missed out a place note field
						undef;
					}
				}
				elsif ($self->{_annotation_data}{type} eq 'person') {
					my @values = sort @{$self->{_annotation_data}{standardised_note}{$standardised_note_field}};
					if (@values > 2) {
						# how do we choose from more than two values
						undef; # BREAKPOINT: improve dispute handling
					}
					else {
						# if one value is blank or 'other' and the other isn't, use the other
						if ($values[0] eq '' && $values[1] eq 'other') {
							$self->{_annotation_data}{standardised_note}{$standardised_note_field} = '';
						}
						elsif ((List::MoreUtils::any {$_ ne '' && $_ ne 'other'} @values)
								&& (List::MoreUtils::any {$_ eq '' || $_ eq 'other'} @values)) {
									# if we get here, one value is blank or other, the other is a useful value
							foreach my $value (@values) {
								if ($value ne 'other' && $value ne '') {
									$self->{_annotation_data}{standardised_note}{$standardised_note_field} = $value;
								}
							}
						}
						elsif ($standardised_note_field eq 'rank') {
							if ((List::MoreUtils::any {$_ eq 'Second Lieutenant'} @values)
								&& (List::MoreUtils::any {$_ eq 'Lieutenant'} @values)) {
									# if we get here, most likely some people didn't notice the '2' superscripts
									$self->{_annotation_data}{standardised_note}{$standardised_note_field} = 'Second Lieutenant';
							}
						}
						elsif ($standardised_note_field eq 'reason') {
							if ((List::MoreUtils::any {$_ eq 'returned_leave'} @values)
									&& (List::MoreUtils::any {$_ eq 'returned_hospital'} @values)) {
										$self->{_annotation_data}{standardised_note}{$standardised_note_field} = 'returned_leave';
							}
						}
						else {
							# how do we choose from this combo of values?
							# BREAKPOINT: improve dispute handling
							$self->{_annotation_data}{standardised_note}{$standardised_note_field} = '';
						}
					}
				}
				else {
					# BREAKPOINT: improve dispute handling
					$self->{_annotation_data}{standardised_note}{$standardised_note_field} = '';
				}
			}
			if (ref($self->{_annotation_data}{standardised_note}{$standardised_note_field}) eq 'ARRAY') {
				# BREAKPOINT: improve dispute handling
				$self->{_annotation_data}{standardised_note}{$standardised_note_field} = '';
			}
		}
	}
	elsif (ref($self->{_annotation_data}{standardised_note}) eq 'ARRAY') {
		# BREAKPOINT: improve dispute handling
		undef;
	}
}

sub DESTROY {
	my ($self) = @_;
	$self->{_page} = undef;
}

1;