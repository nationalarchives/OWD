package OWD::Diary;
use strict;
use warnings;
use OWD::Page;

my $debug = 2;
my $date_lookup = {};

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
		my $page_date_lookup = $date_lookup->{$page_num}; # the section of the $date_lookup hash referncing the current page
		my $page_anomalies = {}; # a hash for any anomalies and their y coordinates 
		$page_date_lookup->{0}{friendly} = $current_date;
		$page_date_lookup->{0}{sortable} = $current_sortable_date;
		if (defined $page->{_clusters}{diaryDate}) {
			# TODO: the next loop will work better if the clusters are dealt with in ascending 
			# y co-ordinate order.
			foreach my $cluster (@{$page->{_clusters}{diaryDate}}) {
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
						$page_date_lookup->{$date_y_coord}{friendly} = $selected_value;
						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($selected_value);
						$page_date_lookup->{$date_y_coord}{cluster}  = $cluster;
						my $error = {
							'type'		=> 'cluster_error; disputed_value_tie',
							'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed. Resolved by reference to undisputed neighbouring clusters',
						};
						$self->data_error($error);
						next;
					}
					if (defined($page_date_lookup->{$date_y_coord})) {
						# we already have a date for this page number and row.
						if (ref($page_date_lookup->{$date_y_coord}) eq 'HASH' 
							&& $page_date_lookup->{$date_y_coord}{friendly} ne $consensus_annotation) {
							# if the row has a single date so far, and it's different from the one
							# we've just found, convert this value to an array and deal with it when 
							# the rest of the dates for this page have been processed
							my $value1 = $page_date_lookup->{$date_y_coord};
							$page_date_lookup->{$date_y_coord} = 
								[$value1, {'friendly' => $consensus_annotation, 'sortable' => get_sortable_date($consensus_annotation), 'cluster' => $cluster} ];
							next;
						}
						elsif (ref($page_date_lookup->{$date_y_coord}) eq 'ARRAY') {
							# we have at least three dates for this row. Implement this edge case
							# if it comes up!
							undef;
						}
					}
					$page_date_lookup->{$date_y_coord}{friendly} = $consensus_annotation;
					$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($consensus_annotation);
					$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
					# ^ circular reference?
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
					my $qualified_dates;
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

sub print_text_report {
	my ($self, $fh) = @_;
	print $fh $self->{_group_data}{zooniverse_id}." ".$self->{_group_data}{metadata}{source}."\n";
	print $fh $self->{_group_data}{stats}{total}." pages\n\n";
	foreach my $page (@{$self->{_pages}}) {
		my $doctype		= $page->get_doctype();
		my $page_num	= $page->get_page_num();
		if ($doctype eq 'cover' || $doctype eq 'blank') {
			print $fh "Page $page_num (type $doctype)\n";
		}
		else {
			print $fh "Page $page_num (type $doctype) http://wd3.herokuapp.com/pages/",$page->get_zooniverse_id,'  ','-'x 10,"\n";
			# do we print the consensus data purely chronologically, or chronologically then categorised?
			# try organising clusters by y-coordinates
			my $chrono_clusters;
			foreach my $type (keys %{$page->{_clusters}}) {
				next if $type eq 'diaryDate' or $type eq 'doctype'; # we've used these to create our date_lookup function already
				foreach my $cluster (@{$page->{_clusters}{$type}}) {
					push @{$chrono_clusters->{$cluster->{median_centroid}[1]}}, $cluster;
				}
			}
			foreach my $y_coord (sort keys %$chrono_clusters) {
				my $date = get_date_for($page_num, $y_coord);
				print $fh "  $date->{friendly}\n";
				foreach my $cluster (@{$chrono_clusters->{$y_coord}}) {
					print $fh "    $cluster->{consensus_type}: $cluster->{consensus_value}\n";
				}
			}
		}
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
