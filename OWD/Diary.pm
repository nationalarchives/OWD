package OWD::Diary;
use strict;
use warnings;
use OWD::Page;

sub new {
	my ($self,$_processor, $_group) = @_;
	my $_diary = bless {	},$self;
	my $subjects_ref = [];	# an array of subjects (pages) within the diary, sorted by page number
	my $cur_subjects = $_processor->{coll_subjects}->find({"group.zooniverse_id" => $_group->{zooniverse_id}});
	$cur_subjects->sort({"metadata.page_number" => 1});
	if ($cur_subjects->has_next) {
		while (my $subject = $cur_subjects->next) {
			push @$subjects_ref, OWD::Page->new($_diary,$subject);
		}
	}
	$_diary->{_pages}		= $subjects_ref;
	$_diary->{_group_data}	= $_group;
	$_diary->{_processor}	= $_processor;
	return $_diary;
}

sub load_classifications {
	my ($self) = @_;
	my $diary_return_val = 0;
	foreach my $page (@{$self->{_pages}}) {
		my $return_val = $page->load_classifications();
		if ($return_val) {
			$diary_return_val++;
		}
	}
	return $diary_return_val;
}

sub get_status {
	my ($self) = @_;
	return $self->{_group_data}{state};
}

sub get_raw_tag_type_counts {
	my ($self) = @_;
	my $diary_tag_counts = {};
	foreach my $page (@{$self->{_pages}}) {
		my $page_tag_counts = $page->get_raw_tag_type_counts();
		while (my ($type,$count) = each %$page_tag_counts) {
			$diary_tag_counts->{$type} += $count;
		}
	}
	return $diary_tag_counts;
}

sub get_docref {
	my ($self) = @_;
	return $self->{_group_data}{metadata}{source};
}

sub get_iaid {
	my ($self) = @_;
	return $self->{_group_data}{metadata}{id};
}

sub get_zooniverse_id {
	my ($self) = @_;
	return $self->{_group_data}{zooniverse_id};
}

sub strip_multiple_classifications_by_single_user {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->strip_multiple_classifications_by_single_user();
	}
}

sub report_pages_with_insufficient_classifications {
	my ($self, $min_classifications) = @_;
	if (!$min_classifications) {
		$min_classifications = 5;
	}
	foreach my $page (@{$self->{_pages}}) {
		if ($page->num_classifications() < $min_classifications) {
			#print $page->get_zooniverse_id(), " has fewer than $min_classifications classifications (",$page->num_classifications(),")\n";
			my $logging_db = $self->{_processor}->get_logging_db();
			my $coll_log = $logging_db->get_collection('log');
			$coll_log->insert({
				'diary'			=> {
					'group_id'		=> $self->get_zooniverse_id(),
					'docref'		=> $self->get_docref(),
					'iaid'			=> $self->get_iaid(),
				},
				'page'			=> {
					'subject_id'	=> $page->get_zooniverse_id(),
					'page_num'		=> $page->get_page_num(),
				},
				'type'				=> "insufficient_classifications_for_page",
				'detail'			=> $page->get_zooniverse_id()." has fewer than $min_classifications classifications (".$page->num_classifications().")",
			});
		};
	}
}

sub cluster_tags {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		if ($page->num_classifications() > 0) {
			$page->cluster_tags();
		}
	}	
}

sub DESTROY {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->DESTROY();
	}
}

1;
