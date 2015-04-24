package OWD::Diary;
use strict;
use warnings;
use OWD::Page;
use Data::Dumper;

my $debug = 2;
my $date_lookup = {};
my $place_lookup = {};

my %month = (
	"Jan" => 1,
	"Feb" => 2,
	"Mar" => 3,
	"Apr" => 4,
	"May" => 5,
	"Jun" => 6,
	"Jul" => 7,
	"Aug" => 8,
	"Sep" => 9,
	"Oct" => 10,
	"Nov" => 11,
	"Dec" => 12,
);

sub new {
	my ($class,$_processor, $_group) = @_;
	my $_diary = bless {},$class;
	$_diary->{_group_data}	= $_group;
	$_diary->{_processor}	= $_processor;
	my $subjects_ref = [];	# an array of subjects (pages) within the diary, sorted by page number
	my $cur_subjects = $_processor->{coll_subjects}->find({"group.zooniverse_id" => $_group->{zooniverse_id}});
	$cur_subjects->sort({"metadata.page_number" => 1});
	if ($cur_subjects->has_next) {
		while (my $subject = $cur_subjects->next) {
			push @$subjects_ref, OWD::Page->new($_diary,$subject);
		}
	}
	$_diary->{_pages}		= $subjects_ref;
	return $_diary;
}

sub load_classifications {
	my ($self) = @_;
	my $diary_return_val = 0;
	foreach my $page (@{$self->{_pages}}) {
		#print "  page $page->{_page_data}{metadata}{page_number}\n" if ($debug > 1);
		my $return_val = $page->load_classifications();
		if ($return_val) {
			$diary_return_val++;
		}
	}
	return $diary_return_val;
}

sub load_hashtags {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->load_hashtags();
	}
	return 1;
}

sub get_status {
	my ($self) = @_;
	return $self->{_group_data}{state};
}

sub get_processor() {
	my ($self) = @_;
	return $self->{_processor};
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

sub establish_consensus {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->establish_consensus();
	}	
}

sub data_error {
	my ($self, $error_hash) = @_;
	if (!defined $error_hash->{diary}) {
		$error_hash->{diary} = {
			'group_id'			=> $self->get_zooniverse_id(),
			'iaid'				=> $self->get_iaid(),
			'docref'			=> $self->get_docref(),
		};
	}
	$self->{_processor}->data_error($error_hash);
}

sub create_date_lookup {
	my ($self) = @_;
	my $start_date = $self->{_group_data}{metadata}{start_date};
	my $current_date = $start_date->day()." ".$start_date->month_abbr()." ".$start_date->year();
	my $current_sortable_date = get_sortable_date($current_date);

	#iterate through each page by ascending page number
	foreach my $page (@{$self->{_pages}}) {
		my $page_num = $page->get_page_num();
		$date_lookup->{$page_num} = {};
		my $page_date_lookup = $date_lookup->{$page_num}; # the section of the $date_lookup hash referencing the current page
		my $page_anomalies = {}; # a hash for any anomalies and their y coordinates 
		$page_date_lookup->{0}{friendly} = $current_date;
		$page_date_lookup->{0}{sortable} = $current_sortable_date;
		if (defined $page->{_clusters}{diaryDate}) {
			foreach my $cluster (sort { $a->{median_centroid}[1] <=> $b->{median_centroid}[1] } @{$page->{_clusters}{diaryDate}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					my $date_y_coord = $cluster->{median_centroid}[1];
					if (ref($consensus_annotation) eq 'ARRAY') {
						# we have multiple possible dates for this cluster. Check the difference
						# from the last known date, and use the closest.
						my $smallest_distance_from_last_known_good;
						my $selected_value;
						my $last_known_good_row = _get_previous_date_row_number($page_date_lookup,$date_y_coord);
						while (ref($page_date_lookup->{$last_known_good_row}) ne 'HASH') {
							$last_known_good_row = _get_previous_date_row_number($page_date_lookup,$last_known_good_row);
						}
						foreach my $possible_value (@$consensus_annotation) {
							my $sortable_date = get_sortable_date($possible_value);
							my $distance_from_last_known_good = $sortable_date - $page_date_lookup->{$last_known_good_row}{sortable};
							if ($distance_from_last_known_good >= 0 && (!defined($smallest_distance_from_last_known_good) || $distance_from_last_known_good < $smallest_distance_from_last_known_good)) {
								$selected_value = $possible_value;
								$smallest_distance_from_last_known_good = $distance_from_last_known_good;
							}
						}
						$cluster->{consensus_value} = $selected_value;
						my $error = {
							'type'		=> 'cluster_error; disputed_value_tie',
							'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed. Resolved by reference to undisputed neighbouring clusters',
						};
						$self->data_error($error);
					}
					if (defined($page_date_lookup->{$date_y_coord})) {
						# we already have a date for this page number and row.
						if (ref($page_date_lookup->{$date_y_coord}) eq 'HASH' 
							&& $page_date_lookup->{$date_y_coord}{friendly} ne $consensus_annotation->get_string_value()) {
							# if the row has a single date so far, and it's different from the one
							# we've just found, convert this value to an array and deal with it when 
							# the rest of the dates for this page have been processed
							my $value1 = $page_date_lookup->{$date_y_coord};
							$page_date_lookup->{$date_y_coord} = 
								[$value1, {'friendly' => $consensus_annotation->get_string_value(), 'sortable' => get_sortable_date($consensus_annotation->get_string_value()), 'cluster' => $cluster} ];
							next;
						}
						elsif (ref($page_date_lookup->{$date_y_coord}) eq 'ARRAY') {
							# we have at least three dates for this row.
							# Add the new date to the existing array for dealing with later
							push @{$page_date_lookup->{$date_y_coord}}, {'friendly' => $consensus_annotation->get_string_value(), 'sortable' => get_sortable_date($consensus_annotation->get_string_value()), 'cluster' => $cluster};
						}
					}
					else {
#						$page_date_lookup->{$date_y_coord}{friendly} = $selected_value;
#						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($selected_value);
						
						$page_date_lookup->{$date_y_coord}{friendly} = $consensus_annotation->get_string_value();
						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($page_date_lookup->{$date_y_coord}{friendly});
						$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
						# ^ circular reference?
					}
				}
			}
			my @rows = sort {$a <=> $b} keys %$page_date_lookup;
			# resolve any instances of multiple dates for a row here.
			foreach my $row (@rows) {
				if (ref($page_date_lookup->{$row}) eq 'ARRAY') {
					# of the two dates we now have for this row, confirm they are both later
					# than the immediately preceeding row. If they both are, select the earlier one
					# on the basis that this case is often caused by an entry like "15th-30th Sep"
					my $previous_row = _get_previous_date_row_number($page_date_lookup,$row,\@rows);
					my $qualified_dates = [];
					foreach my $user_contributed_date (@{$page_date_lookup->{$row}}) {
						if (ref($user_contributed_date) ne "HASH" || ref($page_date_lookup->{$previous_row}) ne "HASH") {
							undef;
						}
						if ($user_contributed_date->{sortable} > $page_date_lookup->{$previous_row}{sortable}) {
							push @$qualified_dates, $user_contributed_date;
						}
					}
					if (@$qualified_dates > 1) {
						# if more than one date meets the "later than the previous date" criteria
						my @user_contributed_dates = sort {$a->{sortable} <=> $b->{sortable}} @$qualified_dates;
						$page_date_lookup->{$row} = $user_contributed_dates[0];
					}
					elsif (@$qualified_dates == 1) {
						# only one of the user contributed dates is later than a previous date
						$page_date_lookup->{$row} = $qualified_dates->[0];
					}
					else {
						# none of the dates qualified - delete this $page_date_lookup row.
						delete $page_date_lookup->{$row};
					}
				}
			}
			# repopulate the array of rows in case it has changed
			@rows = sort {$a <=> $b} keys %$page_date_lookup;
			# get the highest row-numbered date on the page, this will be the first date on the 
			# next page
			my $highest_row_number = $rows[-1];
			$current_date = $page_date_lookup->{$highest_row_number}{friendly};
			$current_sortable_date = $page_date_lookup->{$highest_row_number}{sortable};
		}
	}
}

sub get_date_for {
	my ($page, $row) = @_;
	my $closest_date_row_above = 0;
	foreach my $potential_row_above (keys %{$date_lookup->{$page}}) {
		if ($potential_row_above <= $row && $potential_row_above > $closest_date_row_above) {
			$closest_date_row_above = $potential_row_above;
		} 
	}
	return $date_lookup->{$page}{$closest_date_row_above};
}

sub create_place_lookup {
	my ($self) = @_;
	my $current_place = "";

	#iterate through each page by ascending page number
	foreach my $page (@{$self->{_pages}}) {
		my $page_num = $page->get_page_num();
		$place_lookup->{$page_num} = {};
		my $page_place_lookup = $place_lookup->{$page_num}; # the section of the $place_lookup hash referencing the current page
		my $page_anomalies = {}; # a hash for any anomalies and their y coordinates 
		$page_place_lookup->{0} = $current_place;
		if (defined $page->{_clusters}{place}) {
			foreach my $cluster (sort { $a->{median_centroid}[1] <=> $b->{median_centroid}[1] } @{$page->{_clusters}{place}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					my $place_y_coord = $cluster->{median_centroid}[1];
					if (ref($consensus_annotation) eq 'ARRAY') {
						# we have multiple possible places for this cluster
						# try looking them up in Geonames? Select the nearest to last known location
						undef;
					}
					if (defined($page_place_lookup->{$place_y_coord})) {
						# we already have a place for this page number and row.
						if (ref($page_place_lookup->{$place_y_coord}) eq 'HASH' 
							&& $page_place_lookup->{$place_y_coord} ne $consensus_annotation->get_string_value()) {
							# if the row has a single place so far, and it's different from the one
							# we've just found, convert this value to an array and deal with it when 
							# the rest of the places for this page have been processed
							my $value1 = $page_place_lookup->{$place_y_coord};
							$page_place_lookup->{$place_y_coord} = 
								[$value1, $consensus_annotation->get_string_value()];
							next;
						}
						elsif (ref($page_place_lookup->{$place_y_coord}) eq 'ARRAY') {
							# we have at least three dates for this row.
							# Add the new date to the existing array for dealing with later
							undef
#							push @{$page_place_lookup->{$place_y_coord}}, {'friendly' => $consensus_annotation->get_string_value(), 'sortable' => get_sortable_date($consensus_annotation->get_string_value()), 'cluster' => $cluster};
						}
					}
					else {
						$page_place_lookup->{$place_y_coord} = $consensus_annotation->get_string_value();
					}
				}
			}
			my @rows = sort {$a <=> $b} keys %$page_place_lookup;
			# resolve any instances of multiple places for a row here.
			foreach my $row (@rows) {
				if (ref($page_place_lookup->{$row}) eq 'ARRAY') {
					undef;
				}
			}
			# repopulate the array of rows in case it has changed
			@rows = sort {$a <=> $b} keys %$page_place_lookup;
			# get the highest row-numbered place on the page, this will be the first place on the 
			# next page
			my $highest_row_number = $rows[-1];
			$current_place = $page_place_lookup->{$highest_row_number};
		}
	}
	undef;
}

sub get_place_for {
	my ($page, $row) = @_;
	my $closest_place_row_above = 0;
	foreach my $potential_row_above (keys %{$place_lookup->{$page}}) {
		if ($potential_row_above <= $row && $potential_row_above > $closest_place_row_above) {
			$closest_place_row_above = $potential_row_above;
		} 
	}
	return $place_lookup->{$page}{$closest_place_row_above};
}

sub print_text_report {
	my ($self, $fh) = @_;
	print $fh $self->{_group_data}{zooniverse_id}." ".$self->{_group_data}{metadata}{source}."\n".$self->{_group_data}{name}."\n";
	print $fh $self->{_group_data}{stats}{total}." pages\n\n";
	foreach my $page (@{$self->{_pages}}) {
		my $doctype		= $page->get_doctype();
		my $page_num	= $page->get_page_num();
		if ($doctype eq 'cover' || $doctype eq 'blank') {
			print $fh "Page $page_num (type $doctype)\n";
		}
		else {
			print $fh "Page $page_num (type $doctype) http://wd3.herokuapp.com/pages/",$page->get_zooniverse_id,'  ','-'x 10,"\n";
			if (defined (my $hashtags = $page->get_hashtags())) {
				if (keys %$hashtags > 0) {
					print $fh "  Hashtags: ";
					foreach my $hashtag (reverse sort {$hashtags->{$a} <=> $hashtags->{$b}} keys %$hashtags) {
						print $fh "$hashtag ";
					}
					print $fh "\n";
				}
			}
			# do we print the consensus data purely chronologically, or chronologically then categorised?
			# try organising clusters by y-coordinates
			my $chrono_clusters;
			my $date_boundaries;
			foreach my $type (keys %{$page->{_clusters}}) {
				if ($type eq 'diaryDate') {
					foreach my $cluster (@{$page->{_clusters}{$type}}) {
						if (ref($cluster) eq 'OWD::Cluster' && defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
							$date_boundaries->{$consensus_annotation->get_string_value()} = ${$consensus_annotation->get_coordinates()}[1];
						} 
					}
				}
				next if $type eq 'diaryDate' or $type eq 'doctype'; # we've used these to create our date_lookup function already
				foreach my $cluster (@{$page->{_clusters}{$type}}) {
					push @{$chrono_clusters->{$cluster->{median_centroid}[1]}}, $cluster;
				}
			}
			my $current_date;
			foreach my $y_coord (sort keys %$chrono_clusters) {
				my $date = get_date_for($page_num, $y_coord);
				if (!defined($current_date) || $date->{friendly} ne $current_date) {
					if (!defined($date_boundaries->{$date->{friendly}})) {
						$date_boundaries->{$date->{friendly}} = 0;
					}
					print $fh "  $date->{friendly} ",$date_boundaries->{$date->{friendly}},"\n";
					$current_date = $date->{friendly};
				}
				foreach my $cluster (@{$chrono_clusters->{$y_coord}}) {
					if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
						print $fh "    ",$consensus_annotation->{_annotation_data}{type},":",$consensus_annotation->get_string_value," (",join(",",@{$consensus_annotation->get_coordinates()}),")\n";
					}
				}
			}
		}
	}
}

sub print_tsv_report {
	my ($self, $fh) = @_;
	print $fh "#Unit\tPageNum\tPageID\tDate\tPlace\tAnnotationType\tAnnotationValue\n";
	foreach my $page (@{$self->{_pages}}) {
		my $title 		= $self->{_group_data}{name};
		my $doctype		= $page->get_doctype();
		my $page_num	= $page->get_page_num();
		my $zooniverse_id	= $page->get_zooniverse_id();
		my $date = get_date_for($page_num, 0);
		my $place = get_place_for($page_num, 0);
		my $current_date;
		if (!defined($current_date) || $date->{friendly} ne $current_date) {
			$current_date = $date->{friendly};
		}
		my @hashtags;
		if (defined (my $hashtags = $page->get_hashtags())) {
			@hashtags = keys %$hashtags;
			print $fh "$title\t$page_num\t$current_date\t$place\thashtags\t",join(",",@hashtags),"\n" if (@hashtags > 0);
		}
		my $chrono_clusters;
		my $date_boundaries;
		foreach my $type (keys %{$page->{_clusters}}) {
			if ($type eq 'diaryDate') {
				foreach my $cluster (@{$page->{_clusters}{$type}}) {
					if (ref($cluster) eq 'OWD::Cluster' && defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
						$date_boundaries->{$consensus_annotation->get_string_value()} = ${$consensus_annotation->get_coordinates()}[1];
					} 
				}
			}
			next if $type eq 'diaryDate' or $type eq 'doctype'; # we've used these to create our date_lookup function already
			foreach my $cluster (@{$page->{_clusters}{$type}}) {
				push @{$chrono_clusters->{$cluster->{median_centroid}[1]}}, $cluster;
			}
		}
		foreach my $y_coord (sort keys %$chrono_clusters) {
			$date = get_date_for($page_num, $y_coord);
			$place = get_place_for($page_num, $y_coord);
			if (!defined($current_date) || $date->{friendly} ne $current_date) {
				$current_date = $date->{friendly};
			}
			foreach my $cluster (@{$chrono_clusters->{$y_coord}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					if ((my $type = $consensus_annotation->get_type()) ne 'diaryDate') {
						print $fh "$title\t$page_num\t$zooniverse_id\t$current_date\t$place\t$type\t",$consensus_annotation->get_string_value(),"\n";
					}
				}
			}
		}
	}
}

sub resolve_uncertainty {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->resolve_uncertainty();
	}
	undef;
}

sub publish_to_db {
	my ($self) = @_;
	# clear down any existing references to this diary in the DB
	$self->{_processor}->get_output_db()->get_collection('page')->remove({'group_id' => $self->get_zooniverse_id});
	foreach my $page (@{$self->{_pages}}) {
		my $annotations = [];
		my $output_obj = {
			'zooniverse_id'		=> $page->get_zooniverse_id(),
			'group_id'			=> $self->get_zooniverse_id(),
			'page_num'			=> $page->get_page_num(),
			'image_url'			=> $page->get_image_url(),
		};
		my $clusters_by_type = $page->get_clusters();
		if (defined($clusters_by_type->{doctype}[0])) {
			if (defined(my $consensus_annotation = $clusters_by_type->{doctype}[0]->get_consensus_annotation())) {
				$output_obj->{type} = $consensus_annotation->get_string_value();
			}
			else {
				$output_obj->{type} = 'unknown';
			}
		}
		else {
			undef;
		}
		foreach my $type (sort keys %$clusters_by_type) {
			next if $type eq 'doctype';
			foreach my $cluster (@{$clusters_by_type->{$type}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					push @$annotations, $consensus_annotation->{_annotation_data};
				}
				else {
					undef;
				}
			}
			undef;
		}
		$output_obj->{annotations} = $annotations;
		$self->{_processor}->get_output_db()->get_collection('page')->insert($output_obj);
	}
}

sub _get_previous_date_row_number {
	my ($page_date_lookup, $row, $rows) = @_;
	my @rows;
	if (defined($rows)) {
		@rows = @$rows;
	}
	else {
		@rows = sort {$a <=> $b} keys %$page_date_lookup;
	}
	my $previous_row = 0;
	foreach my $potential_previous_row (@rows) {
		if ($potential_previous_row < $row) {
			$previous_row = $potential_previous_row;
		}
	}
	return $previous_row;
}

sub get_sortable_date {
	my ($date_string) = @_;
	$date_string =~ /(\d+) (\w{3}) (\d{4})/;
	my $sortable_date = int($3.sprintf("%02d",$month{$2}).sprintf("%02d",$1));
	return $sortable_date;
}

sub DESTROY {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->DESTROY();
	}
}

1;
