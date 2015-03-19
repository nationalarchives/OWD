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
	$obj->{range} = 0;
	return $obj;
}

sub add_annotation {
	my ($self, $annotation) = @_;
	push @{$self->{_annotations}}, $annotation;
	my $coords = $self->get_coords();
	$self->{centroid} = calculate_centroid($coords);
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

sub DESTROY {
	my ($self) = @_;
	$self->{_page} = undef;
}
1;