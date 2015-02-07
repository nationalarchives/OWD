package OWD::Processor;
use strict;
use warnings;
use Carp;

use OWD::Diary;

my $group_iterator = 0; # keep track of the next diary in the array of diaries to return.
my $groups_ref = [];	# holds an array of groups (diaries) which can be iterated by index number

sub new {
	my ($self) = @_;
	return bless {}, $self;
}

sub set_database {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database" && $db->get_collection("war_diary_groups")) {
		$self->{database}		= $db;
		$self->{coll_groups}	= $db->get_collection("war_diary_groups");
		$self->{coll_subjects}	= $db->get_collection("war_diary_subjects");
		$self->{coll_classifications}	= $db->get_collection("war_diary_classifications");
		my $cur_groups = $self->{coll_groups}->find();
		if ($cur_groups->has_next) {
			while (my $group = $cur_groups->next) {
				push @$groups_ref, $group;
			}
		}
		return 1;
	}
	else {
		return undef;
	}
}

sub set_output_db {
	my ($self, $db) = @_;
	if ( ref($db) eq "MongoDB::Database") {
		$self->{output_database}	= $db;
		return 1;
	}
	else {
		return undef;
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
		return undef;
	}
}

sub get_logging_db {
	my ($self) = @_;
	return $self->{logging_database};
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
			return undef;
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

1;