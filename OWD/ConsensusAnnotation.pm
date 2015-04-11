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
			|| $self->{_annotation_data}{type} eq 'time') {
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

sub DESTROY {
	my ($self) = @_;
	$self->{_page} = undef;
}

1;