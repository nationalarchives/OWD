package OWD::Page;
use strict;
use OWD::Classification;
use Algorithm::ClusterPoints;

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
	# More than 10,000 classifications are from users who have already classified a particular
	# page. This method checks for this problem and tries to ensure only the best classification
	# is preserved.
	my ($self) = @_;
	my %num_classifications_by;
	my $num_classifications_before_strip = $self->num_classifications();
	my $logging_db = $self->{_diary}{_processor}->get_logging_db();
	my $coll_log = $logging_db->get_collection('log');
	foreach my $classification (@{$self->{_classifications}}) {
		$num_classifications_by{$classification->get_classification_user()}++;
	}
	foreach my $user (keys %num_classifications_by) {
		my $value = $num_classifications_by{$user};
		if ($value > 1) {
			my $best_classification = "";
			#print "$user has multiple classifications for $self->{_page_data}{zooniverse_id}\n";
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
		}
	}
	my $num_classifications_after_strip = $self->num_classifications();
	if ($num_classifications_before_strip > $num_classifications_after_strip) {
		my $diff = $num_classifications_before_strip - $num_classifications_after_strip;
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
			'type'				=> "some_classifications_inadmissable",
			'detail'			=> "$diff / $num_classifications_before_strip",
		});
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

sub cluster_tags {
	my ($self) = @_;
	my $annotations_by_type = {}; # store annotations by type for the main clustering routine
	# ^ destroy this circular ref when the page is destroyed
	# create a first cluster of doctypes, then go through the other classifications
	foreach my $classification (@{$self->{_classifications}}) {
		push @{$annotations_by_type->{doctype}{$classification->get_doctype()}}, $classification;
		my $annotations_by_type_this_classification = $classification->get_annotations_by_type();
		while (my ($type, $annotations) = each %{$annotations_by_type_this_classification}) {
			push @{$annotations_by_type->{$type}}, @$annotations;
		}
	}
	while (my ($type,$annotations) = each %{$annotations_by_type}) {
		undef;
	}
	undef;
}

sub DESTROY {
	my ($self) = @_;
	foreach my $classification (@{$self->{_classifications}}) {
		$classification->DESTROY();
	}
	$self->{_diary} = undef;
}

1;