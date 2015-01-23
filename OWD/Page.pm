package OWD::Page;
use strict;
use OWD::Classification;

sub new {
	my ($self, $_diary, $_subject) = @_;
	return bless {
		'_page_data'	=> $_subject,
		'_diary'		=> $_diary,
	}, $self;
}

sub load_classifications {
	my ($self) = @_;
	my $_classifications = [];
	my $cur_classifications = 
		$self->{_diary}->{_processor}->{coll_classifications}->find(
			{'subjects.zooniverse_id' => $self->{_page_data}->{zooniverse_id} }
		);
	if ($cur_classifications->has_next) {
		while (my $classification = $cur_classifications->next) {
			push @$_classifications, OWD::Classification->new($self,$classification);
		}
		$self->{_classifications} = $_classifications;
		return 1;
	}
	else {
		return undef;
	}
}

sub get_raw_tag_type_counts {
	my ($self) = @_;
	my $page_tag_counts = {};
	foreach my $classification (@{$self->{_classifications}}) {
		my $classification_tag_counts = $classification->get_tag_type_counts();
		while (my ($type,$count) = each %$classification_tag_counts) {
			$page_tag_counts->{$type} += $count;
		}
	}
	return $page_tag_counts;
}

sub strip_multiple_classifications_by_single_user {
	my ($self) = @_;
	my %num_classifications_by;
	foreach my $classification (@{$self->{_classifications}}) {
		$num_classifications_by{$classification->get_classification_user()}++;
	}
	foreach my $user (keys %num_classifications_by) {
		my $value = $num_classifications_by{$user};
		if ($value > 1) {
			my $best_classification = "";
			print "$user has multiple classifications for $self->{_page_data}{zooniverse_id}\n";
			my $logging_db = $self->{_diary}{_processor}->get_logging_db();
			my $coll_log = $logging_db->get_collection('log');
			$coll_log->insert({
				'diary'			=> {
					'group_id'		=> $self->{_diary}->get_zooniverse_id(),
					'docref'		=> $self->{_diary}->get_docref(),
					'iaid'			=> $self->{_diary}->get_iaid(),
				},
				'page'			=> {
					'subject_id'	=> $self->get_zooniverse_id(),
					'page_num'		=> $self->get_page_num(),
				},
				'type'				=> "multiple_classifications_of_page_by_single_user",
				'detail'			=> "$user has multiple classifications for $self->{_page_data}{zooniverse_id}",
			});
			my $classifications_by_user = $self->get_classifications_by($user);
			# iterate through classifications i and i+1. Select the best one
			my $classification_scores = {};
			foreach my $classification (@$classifications_by_user) {
				push @{$classification_scores->{$classification->annotations_count()}},
					{
						'id'			=> $classification->get_mongo_id(),
						'updated_at'	=> $classification->get_updated_at(),
					};
			}
			my $hi_score = (reverse sort keys %$classification_scores)[0];
			if (@{$classification_scores->{$hi_score}} == 1) {
				$best_classification = $classification_scores->{$hi_score}[0]{id};
			}
			else {
				my $latest_timestamp;
				foreach my $classification (@{$classification_scores->{$hi_score}}) {
					if (!defined $latest_timestamp || DateTime->compare($latest_timestamp,$classification->{updated_at}) < 0) {
						$latest_timestamp = $classification->{updated_at};
						$best_classification = $classification->{id};
					}
				}
			}
			my $replacement_classifications;
			for (my $i = 0; $i<@{$self->{_classifications}}; $i++) {
				if ($self->{_classifications}[$i]->get_classification_user() ne $user
					|| ($self->{_classifications}[$i]->get_classification_user() eq $user
						&& $self->{_classifications}[$i]->get_mongo_id eq $best_classification)) {
						push @$replacement_classifications, $self->{_classifications}[$i];
				}
				else {
					$self->{_classifications}[$i]->DESTROY();
				}
			} 
			$self->{_classifications} = $replacement_classifications;
			print "", $self->get_page_num(), " has ", scalar @{$self->{_classifications}}, " classifications after de-duping\n";
		}
	}
}

sub get_classifications_by {
	my ($self, $username) = @_;
	my $classifications_by_user = [];
	foreach my $classification (@{$self->{_classifications}}) {
		if ($classification->get_classification_user() eq $username) {
			push @$classifications_by_user, $classification;
		}
	}
	return $classifications_by_user;
}

sub get_zooniverse_id {
	my ($self) = @_;
	return $self->{_page_data}{zooniverse_id};
}

sub get_page_num {
	my ($self) = @_;
	return $self->{_page_data}{metadata}{page_number};
}

sub num_classifications {
	my ($self) = @_;
	return scalar @{$self->{_classifications}};
}

sub DESTROY {
	my ($self) = @_;
	foreach my $classification (@{$self->{_classifications}}) {
		$classification->DESTROY();
	}
	$self->{_diary} = undef;
}

1;