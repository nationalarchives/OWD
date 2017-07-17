package OWD::Processor;
use strict;
use warnings;
use Carp;

use OWD::Diary;

my $group_iterator = 0; # keep track of the next diary in the array of diaries to return.
my $groups_ref = [];	# holds an array of groups (diaries) which can be iterated by index number

sub new {
	my ($class) = @_;
	return bless {}, $class;
}

sub set_database {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database" && $db->get_collection("war_diary_groups")) {
		$self->{database}		= $db;
		$self->{coll_groups}	= $db->get_collection("war_diary_groups");
		$self->{coll_subjects}	= $db->get_collection("war_diary_subjects");
		$self->{coll_classifications}	= $db->get_collection("war_diary_classifications");
		my $cur_groups = $self->{coll_groups}->find({});
		$cur_groups->fields({'metadata' => 1,'name' => 1,'state' => 1,'stats' => 1,'zooniverse_id' => 1});
		if ($cur_groups->has_next) {
			while (my $group = $cur_groups->next) {
				push @$groups_ref, $group;
			}
		}
		return 1;
	}
	else {
		return;
	}
}

sub set_output_db {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database") {
		$self->{output_database}	= $db;
		return 1;
	}
	else {
		return;
	}
}

sub get_output_db {
	my ($self) = @_;
	return $self->{output_database};
}

sub set_logging_db {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database") {
		$self->{logging_database}	= $db;
		return 1;
	}
	else {
		return;
	}
}

sub get_logging_db {
	my ($self) = @_;
	return $self->{logging_database};
}

sub set_tags_db {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database") {
		$self->{tags_database}	= $db;
		return 1;
	}
	else {
		return;
	}
}

sub set_confirmed_db {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database") {
		$self->{confirmed_database}	= $db;
		return 1;
	}
	else {
		return;
	}
}

sub get_tags_db {
	my ($self) = @_;
	return $self->{tags_database};
}

sub get_confirmed_db {
	my ($self) = @_;
	return $self->{confirmed_database};
}

sub get_confirmed_collection {
	my ($self) = @_;
	return $self->{confirmed_database}->get_collection('confirmed');
}

sub get_delete_collection {
	my ($self) = @_;
	return $self->{confirmed_database}->get_collection('delete');
}

sub get_error_collection {
	my ($self) = @_;
	my $log_db = $self->get_logging_db();
	return $log_db->get_collection('error');
}

sub get_hashtags_collection {
	my ($self) = @_;
	my $hashtags_db = $self->get_tags_db();
	return $hashtags_db->get_collection('discussions');
}

sub get_diary {
	my ($self, $id) = @_;
	if (!defined $id) {
		# in this case return diaries in sequence each time the get_diary function is called.
		# presumably we need to keep track of the last returned diary so that we don't return the same diary again
		#print "\$group_iterator = $group_iterator\n";
		if ($group_iterator < @$groups_ref) {
			my $group = $groups_ref->[$group_iterator++];
			return OWD::Diary->new($self,$group);
		}
		else {
			return;
		}
	}
	elsif ($id =~ m|^GWD|) {
		# OWD Groups (diaries) begin "GWD", in this case return the specific requested diary
		foreach my $group (@$groups_ref) {
			return OWD::Diary->new($self,$group) if $group->{zooniverse_id} eq $id;
		}
		croak "Diary with ID '$id' not found";
	}
	elsif ($id =~ m|^C|) {
		# TNA IAIDs (in the case of war diaries) begin with a C, in this case return the specific diary
		croak "Not implemented!";
	}
	# fetch the relevant group object and call the OWD::Diary constructor to create a diary object for it, then return the
	# object to the caller
}

sub get_key_with_most_array_elements {
	my ($class, $annotations) = @_;
	my $return_val = {};
	foreach my $value (keys %$annotations) {
		my $score = @{$annotations->{$value}};
		if (!defined $return_val->{score} || $return_val->{score} == $score) {
			$return_val->{score} = $score;
			push @{$return_val->{value}}, $value;
		}
		elsif ($return_val->{score} < $score) {
			$return_val->{score} = $score;
			$return_val->{value} = [$value];
		}
	}
	return $return_val;
}

sub data_error {
	my ($self,$error_hash) = @_;
	$self->get_error_collection->insert_one($error_hash);
}

=pod

=head1 NAME

OWD::Processor - base module for OWD processing modules, holding DB connection handles and providing accessor to diaries

=head1 VERSION

v0.1

=head1 SYNOPSIS

use OWD::Processor;

my $owd = OWD::Processor->new();

$owd->set_database(<MongoDB::Database>);	reference to the OWD MongoDB database

$owd->set_output_db(<MongoDB::Database>);	reference to a MongoDB database to which the consensus data should be written

$owd->set_logging_db(<MongoDB::Database>);	reference to a MongoDB database where diagnostic information about the processing can be written

$owd->set_tags_db(<MongoDB::Database>);		if a copy of the Operation War Diary Talk forum database is available it can be included here (the intent is to allow extraction of hashtags and data)

$owd->set_confirmed_db(<MongoDB::Database>); Experimental work in progress

my $diary = $owd->get_diary([string diary_id]);	Returns an OWD::Diary object against which processing operations can be performed

=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=cut

1;