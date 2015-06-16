package OWD::Classification;
use strict;
use Data::Dumper;
use OWD::Annotation;

my $debug = 1;

sub new {
	print "OWD::Classification::new() called\n" if $debug > 2;
	my ($class, $_page, $_classification) = @_;
	my @_annotations;
	my $classification_obj = bless {}, $class;

	if (!defined $_classification->{user_name}) { # Ensure every classification has a user_name
		$_classification->{user_name} = "<anonymous>-$_classification->{user_ip}";
	}
	
	$classification_obj->{_page} = $_page;
	$classification_obj->{_classification_data} = $_classification;
	$classification_obj->{_num_annotations} = 0;
	my $coord_check;
	foreach my $annotation (@{$_classification->{annotations}}) {
		# if the annotation type is "document", create an OWD::Annotation object out of it
		# by rearranging it into a more Annotation-like structure
		if (defined $annotation->{document}) {
			$annotation->{type} = 'doctype';
			$annotation->{note} = $annotation->{document};
			$annotation->{coords} = [0,0];
			delete $annotation->{document};
		}
		$annotation->{id} = $_classification->{subjects}[0]{zooniverse_id}.'_'.$_classification->{user_name}.'_'.$annotation->{coords}[0].'_'.$annotation->{coords}[1];

		# for non-coordinate annotations, push them into the classification metadata
		# as they aren't really volunteer defined anyway
		if (defined $annotation->{finished_at}) {
			$_classification->{finished_at} = $annotation->{finished_at};
		}
		elsif (defined $annotation->{user_agent}) {
			$_classification->{user_agent} = $annotation->{user_agent};
		}
		else {
			print "Processing a page annotation\n" if $debug > 2;
			my $coord_check_string = _coord_check_string($annotation->{coords});
			my $obj_annotation = OWD::Annotation->new($classification_obj,$annotation);
			# record the coordinates to enable duplicate checks.
			if (ref($obj_annotation) eq 'OWD::Annotation') {
				if (defined($coord_check->{$coord_check_string})) {
					# a user has managed to log two annotations in exactly the same place. This is likely
					# a bug and could result in a single user getting two "votes" on what entity is here.
					# Check if the annotations are identical, and if they are, drop and log.
					if ($obj_annotation->is_identical_to($coord_check->{$coord_check_string})) {
						my $error = {
							'type'		=> 'classification_error; duplicate_annotations',
							'detail'	=> $classification_obj->{_classification_data}{user_name}.'\'s classification contains duplicate annotations at $coord_check_string',
						};
						$classification_obj->data_error($error);
						$obj_annotation->DESTROY();
						next;
					}
					else {
						# the two annotations in the same place are actually different.
						# this appears to be another bug, where of the two annotations in the same
						# place, the first one in the array is an "echo" of a previous annotation
						# so far, the second annotation always looks to be the one to keep.
						# Get the @_annotations array element containing the earlier annotation,
						# DESTROY() it to remove the  circular reference, then splice it out of the array
						for (my $i=0; $i<@_annotations;$i++) {
							if (_coord_check_string($_annotations[$i]->{_annotation_data}{coords}) eq $coord_check_string) {
								$_annotations[$i]->DESTROY();
								splice(@_annotations,$i,1);
							}
						}
						my $error = {
							'type'		=> 'classification_error; two_annotations_at_same_coord',
							'detail'	=> $classification_obj->{_classification_data}{user_name}.'\'s classification contains two annotations at $coord_check_string',
						};
						$classification_obj->data_error($error);
					}
				}
				push @_annotations, $obj_annotation;
				$coord_check->{$coord_check_string} = $obj_annotation;
			}
		}
		$classification_obj->{_num_annotations}++;
		print "Annotation object created and added to classification\n" if $debug > 2;
	}
	delete $_classification->{annotations}; # separate the individual annotations from the classification object
	$classification_obj->{_annotations} = \@_annotations;
	undef %$coord_check;
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

sub get_page {
	my ($self) = @_;
	return $self->{_page};
}

sub data_error {
	my ($self, $error_hash) = @_;
	if (!defined $error_hash->{classification}) {
		$error_hash->{classification} = {
			'user_name'		=>  $self->get_classification_user(),
		};
	}
	$self->{_page}->data_error($error_hash);
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

sub get_annotations_count {
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

sub _coord_check_string {
	my ($coords) = @_;
	return $coords->[0].'_'.$coords->[1];
}

sub DESTROY {
	my ($self) = @_;
	foreach my $annotation (@{$self->{_annotations}}) {
		if (ref($annotation) eq 'OWD::Annotation') {
			$annotation->DESTROY();
		}
		else {
			undef;
		}
	}
	$self->{_page} = undef;
}

1;