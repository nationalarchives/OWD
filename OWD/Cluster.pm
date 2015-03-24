package OWD::Cluster;
use strict;
use Data::Dumper;

sub new {
	my ($class, $page, $annotation) = @_;
	my $obj = bless {}, $class;
	$obj->{_page} = $page;
	# ^ remember to delete this circular reference
	push @{$obj->{_annotations}}, $annotation;
	$obj->{centroid} = $annotation->{_annotation_data}{coords};
	$obj->{median_centroid} = $annotation->{_annotation_data}{coords};
	$obj->{range} = 0;
	return $obj;
}

sub add_annotation {
	my ($self, $annotation) = @_;
	push @{$self->{_annotations}}, $annotation;
	my $coords = $self->get_coords();
	$self->{centroid} = calculate_centroid($coords);
	$self->{median_centroid} = calculate_median_centroid($coords);
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

sub get_first_annotation {
	my ($self) = @_;
	return $self->{_annotations}[0];
}

sub distance {
	my ($coord1,$coord2) = @_;
	return sqrt( ( ($coord1->[0] - $coord2->[0])**2 ) + ( ($coord1->[1] - $coord2->[1])**2) );
}

sub establish_consensus {
	my ($self) = @_;
	my %value_counts;
	my $note_type = 'SCALAR';
	$self->{consensus_type} = $self->{_annotations}[0]{_annotation_data}{type};
	if ($self->{consensus_type} ne 'doctype' && $self->{consensus_type} ne 'diaryDate') {
		undef;
	} 
	foreach my $annotation (@{$self->{_annotations}}) {
		$self->{consensus_type} = $annotation->{_annotation_data}{type};
		if (ref($annotation->{_annotation_data}{standardised_note}) eq 'HASH') {
			$note_type = 'HASH';
			foreach my $key (keys %{$annotation->{_annotation_data}{standardised_note}}) {
				$value_counts{$key}{$annotation->{_annotation_data}{standardised_note}{$key}}++;
			}
		}
		else {
			$value_counts{$annotation->{_annotation_data}{standardised_note}}++;
		}
	}
	if ($note_type eq 'SCALAR') {
		my @values = reverse sort { $value_counts{$a} <=> $value_counts{$b} } keys %value_counts;
		if ($value_counts{$values[0]} > 1) {
			# not a lonely cluster, check if there's a tie
			if ($value_counts{$values[0]} > $value_counts{$values[1]}) {
				# consensus
				$self->{consensus_value} = $values[0];
			}
			else {
				# tie for consensus
				my $tied_score = $value_counts{$values[0]};
				foreach my $value (@values) {
					push @{$self->{consensus_value}}, $value if $value_counts{$value} == $tied_score;
				} 
			}
		}
		else {
			# lonely cluster
		}
	}
	else {
		foreach my $key (keys %value_counts) {
			my @values = reverse sort { $value_counts{$key}{$a} <=> $value_counts{$key}{$b} } keys %{$value_counts{$key}};
			if ($value_counts{$values[0]} > 1) {
				# not a lonely cluster, check if there's a tie
				if ($value_counts{$values[0]} > $value_counts{$values[1]}) {
					# consensus
					$self->{consensus_value}{$key} = $values[0];
				}
				else {
					# tie for consensus
					my $tied_score = $value_counts{$key}{$values[0]};
					foreach my $value (@values) {
						push @{$self->{consensus_value}{$key}}, $value if $value_counts{$value} == $tied_score;
					} 
				}
			}
			else {
				# lonely cluster
			}
			
		}
	}
}

sub get_consensus_annotation {
	my ($self) = @_;
	if (defined($self->{consensus_value})) {
		return $self->{consensus_value};
	}
	else {
		return undef;
	}
}

sub DESTROY {
	my ($self) = @_;
	$self->{_page} = undef;
}
1;