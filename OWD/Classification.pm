package OWD::Classification;
use strict;
use Data::Dumper;
use OWD::Annotation;

sub new {
	my ($self, $_page, $_classification) = @_;
	my @_annotations;
	my $classification_obj = bless {}, $self;
	foreach my $annotation (@{$_classification->{annotations}}) {
		# Only create annotation objects for the tags that have x/y co-ords. The other bits that Zooniverse are putting
		# into Annotation tags have only one value per volunteer per page, so I've moved them into the Classification object.
		if (defined $annotation->{document}) {
			$_classification->{doctype} = $annotation->{document};
		}
		elsif (defined $annotation->{finished_at}) {
			$_classification->{finished_at} = $annotation->{finished_at};
		}
		elsif (defined $annotation->{user_agent}) {
			$_classification->{user_agent} = $annotation->{user_agent};
		}
		else {
			push @_annotations, OWD::Annotation->new($classification_obj,$annotation);
		}
	}
	delete $_classification->{annotations}; # separate the individual annotations from the classification object
	
	if (!defined $_classification->{user_name}) { # Ensure every classification has a user_name
		$_classification->{user_name} = "<anonymous>-$_classification->{user_ip}";
	}
	
	$classification_obj->{_page} = $_page;
	$classification_obj->{_classification_data} = $_classification;
	$classification_obj->{_annotations} = \@_annotations;
	return $classification_obj;
}

sub get_tag_type_counts {
	my ($self) = @_;
	my $tag_stats = {};
	foreach my $annotation (@{$self->{_annotations}}) {
		$tag_stats->{$annotation->get_type()}++;
	}
	return $tag_stats;
}

sub get_classification_user {
	my ($self) = @_;
	return $self->{_classification_data}{user_name};
}

sub compare_classifications {
	my ($self,$other) = @_;
	print "0: $self->{_classification_data}{finished_at}\n";
	print "1: $other->{_classification_data}{finished_at}\n";
	print "0: ",scalar(@{$self->{_annotations}}), " annotations\n";
	print "1: ",scalar(@{$other->{_annotations}}), " annotations\n";
	print "0:\n";
	print Dumper $self->get_tag_type_counts();
	print "1:\n";
	print Dumper $other->get_tag_type_counts();
}

sub annotations_count {
	my ($self) = @_;
	return scalar @{$self->{_annotations}};
}

sub get_mongo_id {
	my ($self) = @_;
	return $self->{_classification_data}{_id}{value};
}

sub get_updated_at {
	my ($self) = @_;
	return $self->{_classification_data}{updated_at};
}

sub get_doctype {
	my ($self) = @_;
	return $self->{_classification_data}{doctype};
}

sub get_annotations_by_type {
	my ($self) = @_;
	my $annotations_by_type = {};
	foreach my $annotation (@{$self->{_annotations}}) {
		push @{$annotations_by_type->{$annotation->get_type()}}, $annotation;
	}
	return $annotations_by_type;
	# ensure this reference is destroyed to prevent memory leak
}

sub DESTROY {
	my ($self) = @_;
	foreach my $annotation (@{$self->{_annotations}}) {
		$annotation->DESTROY();
	}
	$self->{_page} = undef;
}

1;