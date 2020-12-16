package OWD::Cluster;
use strict;
use Data::Dumper;
use OWD::ConsensusAnnotation;
use Log::Log4perl;
use constant {
	TIE_FOR_MOST_POPULAR_VALUE	=> 1,
	LONE_USER_SUBMITTED			=> 2,
	PLURALITY_CONSENSUS			=> 3,
	MAJORITY_CONSENSUS			=> 4,
	UNANIMOUS_CONSENSUS			=> 5,
};

my $logger = Log::Log4perl->get_logger();

my $core_consensus_fields = {
	'casualties'=> {
						'died'		=> LONE_USER_SUBMITTED,
						'killed'	=> LONE_USER_SUBMITTED,
						'missing'	=> LONE_USER_SUBMITTED,
						'prisoner'	=> LONE_USER_SUBMITTED,
						'sick'		=> LONE_USER_SUBMITTED,
						'wounded'	=> LONE_USER_SUBMITTED,
	},
	'mapRef'	=> {
						'date'		=> LONE_USER_SUBMITTED,
						'scale'		=> LONE_USER_SUBMITTED,
						'sheet'		=> LONE_USER_SUBMITTED,
	},
	'person'	=> {
						'surname'	=> PLURALITY_CONSENSUS,
	},
	'place'		=> {
						'place'		=> PLURALITY_CONSENSUS,
	},
	'reference'	=> {
						'reference'	=> LONE_USER_SUBMITTED,
	},
	'unit'		=> {
						'name'		=> LONE_USER_SUBMITTED,
	},
	'diaryDate'	=> PLURALITY_CONSENSUS,
};

my $diaryDate_y_axis_skew = -2; # keep this in sync with the similar value in Page.pm

sub new {
	$logger->trace("OWD::Cluster::new() called");
	my ($class, $page, $annotation) = @_;
	my $obj = bless {}, $class;
	$obj->{_page} = $page;
	# ^ remember to delete this circular reference
	push @{$obj->{_annotations}}, $annotation;
#	$obj->{centroid} = $annotation->{_annotation_data}{coords};
#	$obj->{median_centroid} = $annotation->{_annotation_data}{coords};
	my $coords = $obj->get_coords();
	$obj->{centroid} = calculate_centroid($coords);
	$obj->{median_centroid} = calculate_median_centroid($coords);
	$obj->{type} = $annotation->{_annotation_data}{type};
	# annotations often end up one or two y-axis units above the date that refers to them
	# artificially nudge diaryDate types up the page by two y-axis units to try to reduce 
	# the incidence of this happening. (moving up the y axis in this case means subtracting
	# from the y axis value because the origin is the top left corner of the page
	if ($obj->{type} eq 'diaryDate') {
		$obj->{median_centroid}[1] += $diaryDate_y_axis_skew;
		$obj->{median_centroid}[1] = 0 if $obj->{median_centroid}[1] < 0;
		$obj->{centroid}[1] += $diaryDate_y_axis_skew;
		$obj->{centroid}[1] = 0 if $obj->{centroid}[1] < 0;
	}
	$obj->{range} = 0;
	return $obj;
}

sub add_annotation {
	my ($self, $annotation) = @_;
	push @{$self->{_annotations}}, $annotation;
	my $coords = $self->get_coords();
	$self->{centroid} = calculate_centroid($coords);
	$self->{median_centroid} = calculate_median_centroid($coords);
	if ($self->{type} eq 'diaryDate') {
		$self->{median_centroid}[1] += $diaryDate_y_axis_skew;
		$self->{centroid}[1] += $diaryDate_y_axis_skew;
	}
	$self->{range} = $self->get_range($coords);
}

sub get_coords {
	my ($self) = @_;
	my $coords;
	foreach my $annotation (@{$self->{_annotations}}) {
		push @$coords, $annotation->{_annotation_data}{coords};
	}
	return $coords;
}

sub get_centroid {
	my ($self) = @_;
	return $self->{centroid};
}

sub get_median_centroid {
	my ($self) = @_;
	return $self->{median_centroid};
}

sub calculate_centroid {
	my ($coords) = @_;
	my $num_coords = scalar(@$coords);
	my ($x_sum, $y_sum);
	foreach my $coord (@$coords) {
		$x_sum += $coord->[0];
		$y_sum += $coord->[1];
	}
	return [$x_sum / $num_coords, $y_sum / $num_coords];
}

sub calculate_median_centroid {
	my ($coords) = @_;
	my ($x_coords,$y_coords);
	foreach my $coord (@{$coords}) {
		push @{$x_coords}, $coord->[0];
		push @{$y_coords}, $coord->[1];
	}
	return [ int(median(@{$x_coords})), int(median(@{$y_coords})) ];
}

sub median
{
    my @vals = sort {$a <=> $b} @_;
    my $len = @vals;
    if($len%2) #odd?
    {
        return $vals[int($len/2)];
    }
    else #even
    {
        return ($vals[int($len/2)-1] + $vals[int($len/2)])/2;
    }
}


sub get_range {
	my ($self, $coords) = @_;
	my $max = 0;
	foreach my $coord (@$coords) {
		if ((my $distance = distance($coord, $self->{centroid})) > $max) {
			$max = $distance;
		}
	}
	return $max;
}

sub get_type {
	my ($self, $coords) = @_;
	return $self->{type};
}

sub get_first_annotation {
	my ($self) = @_;
	return $self->{_annotations}[0];
}

sub distance {
	my ($coord1,$coord2) = @_;
	return sqrt( ( ($coord1->[0] - $coord2->[0])**2 ) + ( ($coord1->[1] - $coord2->[1])**2) );
}

sub has_contributor {
	my ($self,$contributor) = @_;
	foreach my $annotation (@{$self->{_annotations}}) {
		if ($annotation->get_classification()->get_classification_user() eq $contributor) {
			return 1;
		}
	}
	return 0;
}

sub establish_consensus {
	my ($self) = @_;

	# Start building a consensus annotation structure to be blessed as a ConsensusAnnotation later
	my $consensus_annotation;
	$consensus_annotation->{type} = $self->{_annotations}[0]->get_type();
	$consensus_annotation->{coords} = $self->{median_centroid};
	my $value_counts = $self->_get_annotation_value_scores(); # count up each note value to judge consensus

	my $enough_consensus = 1; # we'll unset this if an annotation fails consensus requirements
	my $status_of_field;
	my $type = $consensus_annotation->{type};
	$logger->debug("Establishing Consensus for a $type cluster at ", join(",",@{$consensus_annotation->{coords}}[0,1]));
	my $error;
	my $note_type;
	if (ref($value_counts->{ (keys %$value_counts)[0] }) ne "HASH") {
		$note_type = 'SCALAR';
	}
	else {
		$note_type = 'HASH';
	}
	
	# Find the most popular value for the note (or for each key of the note, if a more complex annotation)
	if ($note_type eq 'SCALAR') {
		# Order the values, most popular first
		my @values = reverse sort { $value_counts->{$a} <=> $value_counts->{$b} } keys %$value_counts;
		if (@values > 1 && $values[0] eq '') {
			$logger->error("The most popular value for this field of the cluster is blank, although there are other less popular non-blank fields");
		}
		if ($value_counts->{$values[0]} > 1) {
			# more than one user agreed on a value, check if there was unanimity on the value 
			if (defined($values[1])) {
				# there are at least two opinions for this note field
				if ($value_counts->{$values[0]} > $value_counts->{$values[1]}) {
					$status_of_field = PLURALITY_CONSENSUS;
					$consensus_annotation->{standardised_note} = $values[0];
				}
				else {
					$status_of_field = TIE_FOR_MOST_POPULAR_VALUE;
					# if we get to here we need to make $consensus_annotation an array of possible annotations
					# to possibly unpick later with more context
					my $tied_score = $value_counts->{$values[0]};
					foreach my $value (@values) {
						# for each value with the top (tied) score, add it to an array for the cluster consensus for this field
						push @{$consensus_annotation->{standardised_note}}, $value if $value_counts->{$value} == $tied_score;
					}
					$logger->debug("More than one value for a cluster field were tied");
					$error = {
						'type'		=> 'cluster_error; value_tie',
						'detail'	=> "the most popular value for the \'$type\' cluster was a tie of two or more different values",
					};
				}
			}
			else {
				# Users agreed on a single value for a field
				$consensus_annotation->{standardised_note} = $values[0];
				$status_of_field = UNANIMOUS_CONSENSUS;
			}
		}
		else {
			if (@values > 1) {
				# there are several possible values with one vote each
				$status_of_field = TIE_FOR_MOST_POPULAR_VALUE;
				my $tied_score = $value_counts->{$values[0]};
				foreach my $value (@values) {
					push @{$consensus_annotation->{standardised_note}}, $value if $value_counts->{$value} == $tied_score;
				}
				$logger->debug("More than one value for a cluster field were tied");
				$error = {
					'type'		=> 'cluster_error; value_tie',
					'detail'	=> "the most popular value for the \'$type\' cluster was a tie of two or more different values",
				};
			}
			else {
				# lonely cluster
				$status_of_field = LONE_USER_SUBMITTED;
				$logger->debug("Lonely $type annotation cluster");
#				The error state below is already logged at the cluster_tags() stage
#				$error = {
#					'type'				=> 'cluster_error; lonely_cluster',
#					'standardised_note'	=> $self->{_annotations}[0]->get_string_value(),
#					'annotation_id'		=> $self->{_annotations}[0]->get_id(),
#					'detail'			=> 'cluster consists of a single annotation only, not enough for a consensus',
#				};
			}
		}
		if (defined($core_consensus_fields->{ $type })) {
			# if core consensus fields are defined, check they are met before trying to create a
			# ConsensusAnnotation. If they aren't, log an error at this stage
			if ($status_of_field < $core_consensus_fields->{$type}) {
				$enough_consensus = 0;  
			}
		}
		else {
			if ($status_of_field < PLURALITY_CONSENSUS) {
				$enough_consensus = 0;  
			}
#			Experimented with resolving diaryDate disputes here, better to do them later with more context than
#			just the current page.
#			if ($type eq 'diaryDate' && $self->{_page}->get_page_num() == 18) {
#				print $self->{_page}->get_page_num(),"\n";
#				undef;
#			}
		}
	}
	else {
		# if we have a multi-value note field (eg place, person, etc) we don't have to find consensus
		# on all keys, for each annotation type, a core set of fields are important, if there is no
		# consensus on (for example) the 'ui-id-1' accidental place field, no need to worry or flag it
		# as a lonely cluster
		my $is_lonely_cluster = 0;
		foreach my $key (keys %$value_counts) {
			my @values = reverse sort { $value_counts->{$key}{$a} <=> $value_counts->{$key}{$b} } keys %{$value_counts->{$key}};
			if (@values > 1 && $values[0] eq '') { # if there are multiple potential values but the most popular is
				                                   # the empty string, strip that entry to select the most popular non-blank
				shift @values;
				delete $value_counts->{$key}{''};
			}
			if ($value_counts->{$key}{$values[0]} > 1) {
				# not a lonely cluster, check if there's a tie
				if (defined($values[1])) {
					# there are at least two opinions for this note field
					if ($value_counts->{$key}{$values[0]} > $value_counts->{$key}{$values[1]}) {
						$status_of_field->{$key} = PLURALITY_CONSENSUS;
						$consensus_annotation->{standardised_note}{$key} = $values[0];
					}
					else {
						# if there is a tie for the most popular value, we record each of the tied values for resolution later.
						$status_of_field->{$key} = TIE_FOR_MOST_POPULAR_VALUE;
						my $tied_score = $value_counts->{$key}{$values[0]};
						foreach my $value (@values) {
							push @{$consensus_annotation->{standardised_note}{$key}}, $value if $value_counts->{$key}{$value} == $tied_score;
						}
						my $value_string = join "|",@{$consensus_annotation->{standardised_note}{$key}};
						push @$error, {
							'type'		=> 'cluster_error; value_tie',
							'detail'	=> "the most popular value for the \'$type:$key\' cluster was a tie of two or more different values ($value_string)",
						};
					}
				}
				else {
					$status_of_field->{$key} = UNANIMOUS_CONSENSUS;
					$consensus_annotation->{standardised_note}{$key} = $values[0];
				}
			}
			else {
				if (keys %{$value_counts->{$key}} > 1) {
					$status_of_field->{$key} = TIE_FOR_MOST_POPULAR_VALUE;
					my $tied_score = $value_counts->{$key}{$values[0]};
					foreach my $value (@values) {
						push @{$consensus_annotation->{standardised_note}{$key}}, $value if $value_counts->{$key}{$value} == $tied_score;
					}
					my $value_string = join "|",@{$consensus_annotation->{standardised_note}{$key}};
					push @$error, {
						'type'		=> 'cluster_error; value_tie',
						'detail'	=> "the most popular value for the \'$type:$key\' cluster was a tie of two or more different values ($value_string)",
					};
				}
				else {
					$is_lonely_cluster = 1;
					$consensus_annotation->{standardised_note}{$key} = $values[0];
					$status_of_field->{$key} = LONE_USER_SUBMITTED;
				}
			}
#			The following error is already logged at the cluster_tags stage
#			push @$error, {
#				'type'		=> 'cluster_error; lonely_cluster',
#				'standardised_note'	=> $self->{_annotations}[0]->get_string_value(),
#				'annotation_id'		=> $self->{_annotations}[0]->get_id(),
#				'detail'	=> 'cluster consists of a single annotation only, not enough for a consensus',
#			} if $is_lonely_cluster;
		}
		# Check here if there was enough consensus to make a meaningful consensus annotation
		if (defined($core_consensus_fields->{ $type })) {
			$logger->debug("Core consensus constraint set for type $type");
			# if core consensus fields are defined, check they are met before trying to create a
			# ConsensusAnnotation. If they aren't, log an error at this stage
			if (ref($core_consensus_fields->{$type}) eq 'HASH') {
				foreach my $field (keys %{$core_consensus_fields->{$type}}) {
					if ($status_of_field->{$field} < $core_consensus_fields->{$type}{$field}) {
						$logger->debug("Core consensus constraint failed on field $field");
						$enough_consensus = 0;
					}
				} 
			}
			else {
				if ($status_of_field < $core_consensus_fields->{$type}) {
					$logger->debug("Core consensus constraint failed");
					$enough_consensus = 0;
				}
			}
		}
		else {
			# There are no consensus constraints on this annotation type. Use a default of PLURALITY_CONSENSUS
			$logger->debug("No core consensus constraint set for type $type");
			$enough_consensus = 1;
			foreach my $value (values %$status_of_field) {
				if ($value < 3) {
					$enough_consensus = 0;
				}
			}
		}
	}
	if ($enough_consensus) {
		# TODO This object needs to be of type ConsensusAnnotation. A consensus annotation doesn't have a
		# parent classification (because it is derived from many classifications). It has a parent cluster,
		# and the cluster links it to a page. It also doesn't need to go through the standardisation routines
		# that a user annotation goes through (we've already done all that!)
		# We should only bless our $consensus_annotation structure as an object if it has a standardised_note
		# field. If it was not possible to get consensus, there's no point creating a ConsensusAnnotation object
		if (defined($consensus_annotation->{standardised_note})) {
			$logger->trace("Generated consensus annotation");
			my $obj_consensus = OWD::ConsensusAnnotation->new($self,$consensus_annotation);
			$self->{consensus_annotation} = $obj_consensus;
		}
	}
	else {
		$logger->debug("Unable to get consensus for $type cluster");
		if (ref($error) eq 'ARRAY') {
			foreach my $error (@$error) {
				$self->data_error($error);
			}
		}
		elsif (defined $error) {
			$self->data_error($error);
		}
	}
}

sub _get_annotation_value_scores {
	my ($self) = @_;
	my $annotations = $self->{_annotations};
	my $value_counts;

	# tally the number of instances of each value to get a "degree of consensus" score
	if (ref($annotations) ne "ARRAY") {
		$logger->error("Found a cluster whose {_annotations} field isn't a cluster");
	}
	foreach my $annotation (@$annotations) {
		if (ref($annotation->{_annotation_data}{standardised_note}) eq 'HASH') {
			foreach my $key (keys %{$annotation->{_annotation_data}{standardised_note}}) {
				$value_counts->{$key}{$annotation->{_annotation_data}{standardised_note}{$key}}++;
			}
		}
		else {
			$value_counts->{$annotation->{_annotation_data}{standardised_note}}++;
		}
	}
	return $value_counts;
}

sub get_consensus_annotation {
	my ($self) = @_;
	if (defined($self->{consensus_annotation})) {
		return $self->{consensus_annotation};
	}
	else {
		return undef;
	}
}

sub get_page {
	my ($self) = @_;
	return $self->{_page};
}

sub count_annotations {
	my ($self) = @_;
	return scalar @{$self->{_annotations}};
}

sub data_error {
	my ($self,$error_hash) = @_;
	if (!defined $error_hash->{cluster}) {
		$error_hash->{cluster} = {
			'location'	=> $self->{median_centroid}[0].','.$self->{median_centroid}[1],
			'type'		=> $self->{type},
		};
	}
	$self->{_page}->data_error($error_hash);
}

sub DESTROY {
	my ($self) = @_;
	$self->{_page} = undef;
}
1;