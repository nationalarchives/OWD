package OWD::Page;
use strict;
use OWD::Classification;
use OWD::Cluster;
use Algorithm::ClusterPoints;
use Data::Dumper;
use Text::LevenshteinXS;
use Carp;

my $debug = 1;

sub new {
	my ($class, $_diary, $_subject) = @_;
	return bless {
		'_page_data'	=> $_subject,
		'_diary'		=> $_diary,
	}, $class;
	# ^ destroy _diary circular ref when the page is destroyed
	
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
				push @{$classification_scores->{$classification->get_annotations_count()}},
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

sub get_diary {
	my ($self) = @_;
	return $self->{_diary};
}

sub get_doctype {
	my ($self) = @_;
	unless (defined($self->{_clusters})) {
		carp("get_doctype called on OWD::Page object before annotations have been clustered");
		return undef;
	}
	return $self->{_clusters}{doctype}[0]{consensus_value};
}

sub num_classifications {
	my ($self) = @_;
	return scalar @{$self->{_classifications}};
}

sub cluster_tags {
	my ($self) = @_;
	# create a data structure of annotations, grouped first by type, then by user
	# For each tag type, use the annotations by the user with the most contributions for that
	# tag type to create a skeleton cluster layout, then try to match the tags of this type
	# from other users to these clusters.
	my $annotations_by_type_and_user = $self->get_annotations_by_type_and_user();
	foreach my $user (keys %{$annotations_by_type_and_user->{doctype}}) {
		#push @{$self->{_clusters}{doctype}}, $annotations_by_type_and_user->{doctype}{$user}[0];
		if (!defined($self->{_clusters}{doctype})) {
			push @{$self->{_clusters}{doctype}}, OWD::Cluster->new($self,$annotations_by_type_and_user->{doctype}{$user}[0]);
		}
		else {
			my $cluster = $self->{_clusters}{doctype}[0];
			$cluster->add_annotation($annotations_by_type_and_user->{doctype}{$user}[0]);
		}
	}
	foreach my $type (keys %$annotations_by_type_and_user) {
		next if $type eq 'doctype';
		# for this tag type, who has the most tags. Use their tags to create the skeleton cluster layout
		my $user_annotations_by_type_popularity = _num_tags_of_type($annotations_by_type_and_user->{$type});
		my $first_user_for_this_type = 1;
		foreach my $num_annotations (reverse sort {$a <=> $b} keys %$user_annotations_by_type_popularity) {
			foreach my $username (keys %{$user_annotations_by_type_popularity->{$num_annotations}}) {
				if ($first_user_for_this_type) {
					# This is the top user for the tag type, so create a new cluster for each of their tags
					foreach my $annotation (@{$user_annotations_by_type_popularity->{$num_annotations}{$username}}) {
						#push @{$self->{_clusters}{$type}}, [$annotation];
						push @{$self->{_clusters}{$type}}, OWD::Cluster->new($self,$annotation);
					}
					$first_user_for_this_type = 0;
				}
				else {
					foreach my $annotation (@{$user_annotations_by_type_popularity->{$num_annotations}{$username}}) {
						$self->_match_annotation_against_existing($annotation);
					}
				}
			}
		}
	}
	foreach my $cluster_type (keys %{$self->{_clusters}}) {
		foreach my $cluster (@{$self->{_clusters}{$cluster_type}}) {
			if (@{$cluster->{_annotations}} <= 1) {
				my $annotation = $cluster->{_annotations}[0];
				my $id = $annotation->get_id();
				my $string = $annotation->get_string_value();
				my $error = {
					'type'		=> 'cluster_error; single_annotation_cluster',
					'detail'	=> "annotation '$id' ($string) can't be clustered with any other annotations",
				};
				$self->data_error($error);
			}
		}
	}
}

sub _match_annotation_against_existing {
	my ($self, $new_annotation) = @_;
	# for each cluster fpr this type so far, try matching new tag to it
	# if they have the same user, reject
	# find the nearest tag that meets "similarity" requirements. If there aren't any, start a new cluster.
	my $type = $new_annotation->get_type();
	my $annotation_distance_from_cluster;
	for (my $cluster_num = 0; $cluster_num <  @{$self->{_clusters}{$type}}; $cluster_num++) {
		# check for distance (x/y)
		# check for similarity (annotation string)
		my $distance = acceptable_distance($type,$new_annotation->get_coordinates(),$self->{_clusters}{$type}[$cluster_num]->get_centroid());
		if (defined($distance)) {
			$annotation_distance_from_cluster->{$cluster_num} = $distance;
		}
	}
	if (defined($annotation_distance_from_cluster)) {
		# select the best cluster
		# sort by cluster distance, then try any potential matches for note string similarity
		my $annotation_has_been_clustered = 0;
		foreach my $cluster_num (sort {$annotation_distance_from_cluster->{$a} <=> $annotation_distance_from_cluster->{$b}} keys %{$annotation_distance_from_cluster}) {
			# to decide whether the two annotations we are comparing are of the same thing we may need to use
			# various criteria
			my $cluster_string = $self->{_clusters}{$type}[$cluster_num]->get_first_annotation()->get_string_value();
			my $new_annotation_string = $new_annotation->get_string_value();
			if (similar_enough($type, $cluster_string, $new_annotation_string)) {
				# add new annotation to this cluster
				$self->{_clusters}{$type}[$cluster_num]->add_annotation($new_annotation);
				$annotation_has_been_clustered = 1;
				last;
			}
			else {
				undef;
			}
		}
		if (!$annotation_has_been_clustered) {
			# if we get to here, though there were nearby clusters, they were too different to be able
			# to add $new_annotation to the cluster. In this case we should create a new cluster.
			push @{$self->{_clusters}{$type}}, OWD::Cluster->new($self,$new_annotation);
		}
	}
	else {
		# create a new cluster
		push @{$self->{_clusters}{$type}}, OWD::Cluster->new($self,$new_annotation);
	}
}

sub acceptable_distance {
	my ($type, $coord1, $coord2) = @_;
	# acceptable difference is a combination of a maximum x distance, a maximum y distance, and a 
	# maximum total distance (because there needs to be more tolerance on the x axis than the y axis)
	my $x_max;
	my $y_max;
	my $dist_max;
	if ($type eq 'person') {
		$x_max = 11;
		$y_max = 3;
		$dist_max = 11;
	}
	elsif ($type eq 'diaryDate') {
		$x_max = 15;
		$y_max = 6;
		$dist_max = 15;
	}
	else {
		$x_max = 9;
		$y_max = 3;
		$dist_max = 8;
	}
	my $x_dist = abs($coord1->[0] - $coord2->[0]);
	my $y_dist = abs($coord1->[1] - $coord2->[1]);
	if ($x_dist <= $x_max && $y_dist <= $y_max) {
		my $diff = distance($coord1,$coord2);
		if ($diff <= $dist_max) {
			return $diff;
		}
		else {
			return undef;
		}
	}
	else {
		return undef;
	}
}

sub distance {
	my ($coord1,$coord2) = @_;
	return sqrt( ( ($coord1->[0] - $coord2->[0])**2 ) + ( ($coord1->[1] - $coord2->[1])**2) );
}

sub similar_enough {
	my ($type, $string1, $string2) = @_;
	my $lc_string1 = lc($string1);
	my $lc_string2 = lc($string2);
	if ($type eq 'activity') {
		# don't bother checking for matching activity type, the selection of values is discrete and
		# there are lots of disputes. Proximity is good enough.
		return 1;
	}
	else {
		my $max_lev_score;
		if (length($string1) < 4) {
			$max_lev_score = 0;
		}
		else {
			$max_lev_score = length($string1)/2;
		}
		if (Text::LevenshteinXS::distance($lc_string1,$lc_string2) > $max_lev_score) {
			return 0;
		}
		else {
			return 1;
		}
	}
}

sub establish_consensus {
	my ($self) = @_;
	foreach my $type (keys %{$self->{_clusters}}) {
		foreach my $cluster (@{$self->{_clusters}{$type}}) {
			$cluster->establish_consensus();
		}
	}
}

sub _num_tags_of_type {
	my ($annotations_grouped_by_user) = shift;
	my $user_annotations_by_num_uses;
	foreach my $user (keys %$annotations_grouped_by_user) {
		my $num_tags_for_user = @{$annotations_grouped_by_user->{$user}};
		$user_annotations_by_num_uses->{$num_tags_for_user}{$user} = $annotations_grouped_by_user->{$user};
	}
	return $user_annotations_by_num_uses;
}

sub cluster_tags_using_cluster_algorithm {
	# separating annotations that have been too aggressively clustered is REALLY complicated. Going
	# back to previous clustering algorithm
	# DELETE THIS?
	my ($self) = @_;
	my $annotations_by_type = $self->get_annotations_by_type(); # store annotations by type for the main clustering routine
	# ^ destroy this circular ref when the page is destroyed
	foreach my $type (keys %$annotations_by_type) {
		my $annotations = $annotations_by_type->{$type};
		# Use separate tolerances for diaryDate co-ordinates as some users were ensuring that
		# the ruler correctly divided entries, while others were tagging the precise location
		# of the date entry. Both are right, but those who used the ruler method will produce
		# more accurate results.
		my $clp;
		if ($type eq 'diaryDate') {
			$clp = Algorithm::ClusterPoints->new(
						dimension		=> 2,
						radius			=> 20.0,
						minimum_size 	=> 1,
						scales			=> [1,4],
			);
		}
		else {
			$clp = Algorithm::ClusterPoints->new(
						dimension		=> 2,
						radius			=> 24.0,
						minimum_size 	=> 1,
						scales			=> [2,7],
			);
		}
		if ($type eq "doctype") {
			# treat this annotation type separately as it doesn't have co-ordinates and 
			# only occurs once per user per page.
			#my $consensus_key = OWD::Processor->get_key_with_most_array_elements($annotations);
			push @{$self->{_clusters}{doctype}}, $annotations_by_type->{$type};
		}
		else {
			# We have filtered the options by type, now we should be confident enough to
			# cluster by co-ordinate, then for each cluster, check if the note field is close enough
			foreach my $annotation (@{$annotations}) {
				$clp->add_point(@{$annotation->{_annotation_data}{coords}});
			}
			my @clusters = $clp->clusters_ix;
			foreach my $cluster (@clusters) {
				my $this_cluster;
				foreach my $annotation_number (@{$cluster}) {
					push @{$this_cluster}, $annotations->[$annotation_number];
				}
				push @{$self->{_clusters}{$type}}, $this_cluster;
			}
		}
		# Once we have an initial set of clusters, check for any annotations that
		# have been too aggressively clustered, and move annotations between clusters
		# as appropriate.
		$self->_tidy_clusters();
	}
}

sub _tidy_clusters {
	# This was needed to deal with aggressively clustered annotations when using the ClusterPoints
	# algorithm. DELETE THIS
	my ($self) = @_;
	# Check the annotations in each cluster to see if the same user has more than one annotation
	# If so, chances are we've clustered two entities together by being too lenient on x/y coords.
	# Review each annotation in the cluster, calculating its distance and "similarity" to other
	# annotations in the cluster (or nearby annotations from any cluster?), then optimise the clusters
	# by moving annotations between clusters.
	foreach my $type (keys %{$self->{_clusters}}) {
		next if $type eq 'doctype';
		foreach my $cluster (@{$self->{_clusters}{$type}}) {
			my $cluster_annotations_per_user;
			foreach (my $annotation_number = 0; $annotation_number < @$cluster; $annotation_number++) {
				my $annotation = $cluster->[$annotation_number];
				my $user = $annotation->get_classification()->get_classification_user();
				push @{$cluster_annotations_per_user->{$user}},$annotation_number;
			}
			foreach my $user (keys %$cluster_annotations_per_user) {
				if (@{$cluster_annotations_per_user->{$user}} > 1) {
					my @separate_entities;
					foreach my $annotation_number (@{$cluster_annotations_per_user->{$user}}) {
						push @separate_entities, $cluster->[$annotation_number]->get_coordinates()
					}
					my $shortest_distance = get_shortest_distance_between_points(\@separate_entities);
					undef;
				}
			}
		}
	}
}

sub get_shortest_distance_between_points {
	my ($points) = @_;
	my @distances;
	for (my $i=0; $i<@$points-1; $i++) {
		for (my $j = $i+1; $j < @$points; $j++) {
			push @distances, 
				sqrt( (($points->[$i][0] - $points->[$j][0])^2)
					+(($points->[$i][1] - $points->[$j][1])^2) );
		}
	}
	return (sort @distances)[0];
}

sub get_annotations_by_type {
	my ($self) = @_;
	my $annotations_by_type = {};
	foreach my $classification (@{$self->{_classifications}}) {
		#push @{$annotations_by_type->{doctype}{$classification->get_doctype()}}, $classification;
		my $annotations_by_type_this_classification = $classification->get_annotations_by_type();
		while (my ($type, $annotations) = each %{$annotations_by_type_this_classification}) {
			push @{$annotations_by_type->{$type}}, @$annotations;
		}
	}
	return $annotations_by_type;
}

sub get_annotations_by_type_and_user {
	my ($self) = @_;
	my $annotations_by_type = {};
	foreach my $classification (@{$self->{_classifications}}) {
		my $user = $classification->get_classification_user();
		my $annotations_by_type_this_classification = $classification->get_annotations_by_type();
		foreach my $type (keys %$annotations_by_type_this_classification) {
			$annotations_by_type->{$type}{$user} = $annotations_by_type_this_classification->{$type};
		}
	}
	return $annotations_by_type;
}

sub find_similar_nearby_tags {

	my ($self, $type, $centre) = @_;
	#my $annotations_by_type = 
}

sub data_error {
	my ($self, $error_hash) = @_;
	if (!defined $error_hash->{page}) {
		$error_hash->{page} = {
			'subject_id'		=> $self->get_zooniverse_id(),
			'page_number'		=> $self->get_page_num(),
		};
	}
	$self->{_diary}->data_error($error_hash);
}

sub DESTROY {
	my ($self) = @_;
	foreach my $classification (@{$self->{_classifications}}) {
		$classification->DESTROY();
	}
	if (ref($self->{_clusters}) ne 'ARRAY') {
		undef;
	}
	foreach my $type (keys %{$self->{_clusters}}) {
		foreach my $cluster (@{$self->{_clusters}{$type}}) {
			$cluster->DESTROY();
		}
	}
	$self->{_diary} = undef;
}

1;