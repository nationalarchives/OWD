package OWD::Diary;
use strict;
use warnings;
use OWD::Page;
use Carp;
use Data::Dumper;
use DateTime::Format::Natural;

my $debug = 1;
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
#	my $subjects_ref = [];	# an array of subjects (pages) within the diary, sorted by page number
#	my $cur_subjects = $_processor->{coll_subjects}->find({"group.zooniverse_id" => $_group->{zooniverse_id}});
#	$cur_subjects->fields({'classification_count'=>1,'group'=>1,'location'=>1,'metadata'=>1,'state'=>1,'zooniverse_id'=>1});
#	$cur_subjects->sort({"metadata.page_number" => 1});
#	if ($cur_subjects->has_next) {
#		while (my $subject = $cur_subjects->next) {
#			push @$subjects_ref, OWD::Page->new($_diary,$subject);
#		}
#	}
#	$_diary->{_pages}		= $subjects_ref;
	return $_diary;
}

sub load_pages {
	my ($self) = @_;
	print "Loading page data for current diary\n" if $debug;
	my $subjects_ref = [];	# an array of subjects (pages) within the diary, sorted by page number
	my @stage;
	push @stage,{'stage' => 'find() method call','time' => time()};
	my $cur_subjects = $self->{_processor}->{coll_subjects}->find({"group.zooniverse_id" => $self->get_zooniverse_id()});
	push @stage,{'stage' => 'field_selection','time' => time()};
	$cur_subjects->fields({'classification_count'=>1,'group'=>1,'location'=>1,'metadata'=>1,'state'=>1,'zooniverse_id'=>1});
	push @stage,{'stage' => 'sort','time' => time()};
	$cur_subjects->sort({"metadata.page_number" => 1});
	push @stage,{'stage' => 'has_next','time' => time()};
	if ($cur_subjects->has_next) {
		push @stage,{'stage' => '','time' => time()};
		while (my $subject = $cur_subjects->next) {
			# OWD::Page->new() creates a Page object with the passed metadata
			push @$subjects_ref, OWD::Page->new($self,$subject);
		}
	}
	undef $cur_subjects;
	for (my $stage_num=0; $stage_num<@stage-1;$stage_num++) {
		print $stage[$stage_num]{stage}, ": ", $stage[$stage_num+1]{time} - $stage[$stage_num]{time}, "s\n";
	}
	$self->{_pages}		= $subjects_ref;
	return 1;
}

sub load_classifications {
	print "OWD::Diary::load_classifications() called\n" if $debug > 2;
	my ($self) = @_;
	my $diary_return_val = 0;
	if (!defined $self->{_pages}) {
		$self->load_pages();
	}
	foreach my $page (@{$self->{_pages}}) {
		print "  page ", $page->get_page_num(), "\n";
		my $return_val = $page->load_classifications();
		if ($return_val) {
			$diary_return_val++;
		}
	}
	return $diary_return_val;
}

sub load_hashtags {
	my ($self) = @_;
	if (!defined $self->{_pages}) {
		$self->load_pages();
	}
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
	if (!defined $self->{_pages}) {
		$self->load_pages();
	}
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

sub get_start_date {
	my ($self) = @_;
	return $self->{_group_data}{metadata}{start_date};
}

sub get_zooniverse_id {
	my ($self) = @_;
	return $self->{_group_data}{zooniverse_id};
}

sub strip_multiple_classifications_by_single_user {
	my ($self) = @_;
	if (!defined $self->{_pages}) {
		croak((caller(0))[3]." called before load_pages and load_classifications have been called");
	}
	foreach my $page (@{$self->{_pages}}) {
		$page->strip_multiple_classifications_by_single_user();
	}
}

sub report_pages_with_insufficient_classifications {
	my ($self, $min_classifications) = @_;
	if (!$min_classifications) {
		$min_classifications = 5;
	}
	if (!defined $self->{_pages}) {
		$self->load_pages();
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
	if (!defined $self->{_pages}) {
		$self->load_pages();
	}
	foreach my $page (@{$self->{_pages}}) {
		if ($page->num_classifications() > 0) {
			$page->cluster_tags();
		}
	}	
}

sub establish_consensus {
	my ($self) = @_;

	# for pages that have no consensus on diaryDate clusters, it can be impossible to verify the date on the page in isolation
	# but if we keep a running tally of the dates across the whole diary, the correct date can often be implied/inferred from the
	# consensus dates on surrounding pages.
	$self->{date_range} = {};

	if (!defined $self->{_pages}) {
		$self->load_pages();
	}

	foreach my $page (@{$self->{_pages}}) {
		$page->establish_consensus();
	}
	foreach my $page (@{$self->{_pages}}) {
		$self->{date_range}{$page->get_page_num()} = $page->get_date_range();
	}
#	foreach my $page (@{$self->{_pages}}) {
#		$page->resolve_diaryDate_disputes();
#	}
	#$self->report_date_ranges_per_page();
	# After doing a first pass on establishing consensus, we can reprocess the disputed clusters with the consensus clusters elsewhere.
	# In particular, look for no-consensus diaryDate fields and no-consensus person fields.
	# TODO: get the range of consensus diaryDates per page, then use this to inform the decision on disputed dates.
	# Create subs for OWD::Page->get_consensus_date_range()
	$self->create_date_lookup();
	undef;	
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

sub report_date_ranges_per_page {
	my ($self) = @_;
	my $date_parser = DateTime::Format::Natural->new();
	my $base_date = $date_parser->parse_datetime("1 Jan 1914");
	my $filename = $self->get_zooniverse_id()."-date_ranges.tsv";
	open my $ofh, ">",  "output/$filename";
	print $ofh "Page\tdate\n";
	foreach my $page_num (sort {$a <=> $b} keys %{$self->{date_range}}) {
		next if (!defined $self->{date_range}{$page_num}{min});
		foreach ('min','max') {
			my $date = $self->{date_range}{$page_num}{$_};
			print $ofh "$page_num";
			if ($_ eq 'min') { print $ofh "s\t" } else {print $ofh "e\t"; }
			my $dur = $base_date->delta_days($date);
			my $days = $dur->in_units('days');
			print $ofh "$days\n";
		}
	}
	close $ofh;
}

sub create_date_lookup {
	my ($self) = @_;
	my $acceptable_date_jump = 18; # the number of days between two entries in the diary before we trigger
								   # more checking
	my $date_parser = DateTime::Format::Natural->new();
	# This sub runs after the establish_consensus() subroutine, but some diaryDate clusters may not have reached consensus
	# (I've seen examples where there is disagreement about the year for a page where the author neglected to include the year in
	# the dates.)
	# First, get the start date of the diary from the TNA Discovery Catalogue metadata
	my $start_date = $self->get_start_date();
	my $current_date->{friendly} = $start_date->day()." ".$start_date->month_abbr()." ".$start_date->year();
	if ($current_date->{friendly} !~ /^\d{1,2} [a-z]{3} \d{4}/i) {
		undef;
	}
	$current_date->{sortable} = get_sortable_date($current_date->{friendly});
	my $dt_current_date = $date_parser->parse_datetime($current_date->{friendly});

	if (!defined $self->{_pages}) {
		$self->load_pages();
	}

	# iterate through each page by ascending page number. Prepare the object hash reference, $date_lookup for content.
	# It will be keyed by page number then y-coordinate, later allowing a date lookup given those two parameters 
	foreach my $page (@{$self->{_pages}}) {
		my $page_num = $page->get_page_num();
		print "create_date_lookup, page $page_num\n" if $debug > 2;
		$date_lookup->{$page_num} = {};
		my $page_date_lookup = $date_lookup->{$page_num}; # the section of the $date_lookup hash referencing the current page
		
		# Before we process any annotations for the current page, set the date lookup for y-coord 0 to the $current_date value
		# For the first page of the diary, this is set to the StartDate field from the TNA Discovery Catalogue. For subsequent pages,
		# it is set to the last known date from the previous page
		my %temp_var = %{$current_date};
		$page_date_lookup->{0} = \%temp_var; 
		
		# If there are no further dates on the page, this will be the only date lookup for the page. If there are other diaryDate
		# type annotations, iterate through them now, ascending sequentially by y-coordinate
		if (defined $page->{_clusters}{diaryDate}) {
			foreach my $cluster (sort { $a->{median_centroid}[1] <=> $b->{median_centroid}[1] } @{$page->{_clusters}{diaryDate}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					# There was consensus for the date at this cluster
					my $date_y_coord = ($cluster->get_median_centroid())->[1];

					if (ref($consensus_annotation->get_note()) eq 'ARRAY') {
						# we have multiple possible dates for this cluster. Check the difference
						# from the last known date ($current_date), and use the closest.
						my $distance_from_current;
						my $index_of_selected_annotation;
						my $selected_value;
						for (my $i = 0; $i < @{$consensus_annotation->get_note()}; $i++) {
							my $annotation_option = ${$consensus_annotation->get_note()}[$i];
							if ($annotation_option =~ /^\d{1,2} [a-z]{3} \d{4}$/i) {
								my $dt_annotation_option = $date_parser->parse_datetime($annotation_option);
								$distance_from_current->{$i} = $dt_annotation_option->delta_days($dt_current_date)->delta_days();
								if ($distance_from_current->{$i} > $acceptable_date_jump) {
									print "Page ",$page->get_zooniverse_id(),". Most recent date was $current_date->{friendly}. The current non-consensus date option is $annotation_option. The date difference of $distance_from_current->{$i} is greater than the one we determined to be acceptable ($acceptable_date_jump). What do we do here?\n" if $debug > 2;
									next; # ignore this annotation from the list if the date jump is too great.
								}
								if (DateTime->compare($dt_current_date,$dt_annotation_option) <= 0) {
									if (!defined $index_of_selected_annotation
											|| $distance_from_current->{$index_of_selected_annotation} > $distance_from_current->{$i}) {
										$index_of_selected_annotation = $i;
									}
								}	
							}
							else {
								# one of the array of possible dates isn't in the right format
								undef;
							}
						}
						if (!defined $index_of_selected_annotation) {
							# The rules didn't select a favoured annotation - maybe they were all earlier than the latest date we had reached?
							undef;
						}
						else {
							$selected_value = ${$consensus_annotation->get_note()}[$index_of_selected_annotation];
						}
						if (defined $selected_value) {
							$consensus_annotation->set_note($selected_value);
							my $error = {
								'type'		=> 'cluster_error; disputed_value_tie_resolved',
								'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed. Resolved by reference to undisputed neighbouring clusters',
							};
							$self->data_error($error);
						}
						else {
							my $error = {
								'type'		=> 'cluster_error; disputed_value_tie_unresolved',
								'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed with insufficient supporting data to resolve',
							};
							$self->data_error($error);
							undef;
							next; # skip this cluster
						}
					}
					
					# check that the single value we have now is sensible (not too far from the previous date we had.
					my $consensus_date_friendly = $consensus_annotation->get_string_value();
					if ($consensus_date_friendly !~ /^\d{1,2} [a-z]{3} \d{4}$/i) {
						# The consensus date isn't of the right format.
						undef;
					}
					else {
						# Get DateTime objects for $consensus_date_friendly and $current_date, then check for an acceptable distance between them
						my $dt_consensus = $date_parser->parse_datetime($consensus_date_friendly);
						my $diff_days = $dt_consensus->delta_days($dt_current_date)->delta_days();
						if (DateTime->compare($dt_current_date,$dt_consensus) <= 0) {
							# $consensus_date_friendly is later than the $current_date so far (correct order)
							if ($diff_days >= 0 && $diff_days < $acceptable_date_jump) {
								# the $consensus_date_friendly doesn't take a huge jump into the future
								undef;
							}
							elsif ($diff_days >= $acceptable_date_jump && $diff_days < 30) {
								# a suspect datejump, but happens sometimes.
								undef;
							}
							else {
								# $consensus_date_friendly is later than $current_date, but there's a big jump
								undef;
								my $year_length_in_days = 365;
								for (my $year_error_margin = 1; $year_error_margin < 5; $year_error_margin++) {
									my $test_min = $year_length_in_days * $year_error_margin - $acceptable_date_jump;
									my $test_max = $year_length_in_days * $year_error_margin + $acceptable_date_jump;
									if ($diff_days > $test_min && $diff_days < $test_max) {
										print "Page ",$page->get_zooniverse_id(),". $consensus_date_friendly is probably $year_error_margin years out. Current date is $current_date->{friendly}\n" if $debug > 2;
										# then use DateTime math to subtract the appropriate number of years.
										$dt_consensus->subtract( years => $year_error_margin );
										$consensus_annotation->set_note($dt_consensus->day()." ".$dt_consensus->month_abbr()." ".$dt_consensus->year());
										my $error = {
											'type'		=> 'cluster_error; incorrect_year_fixed',
											'detail'	=> 'the diaryDate value \''.$consensus_date_friendly.'\' for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' was fixed by changing the year to make it closer to the previously found date, \''.$current_date->{friendly}.'\'',
										};
										$self->data_error($error);
										last;
									}
								}
								# TODO: Check +/- a month for new dates that seem to be a big jump but only if there is significant dispute of the value anyway
#								my $days_acceptable_diff_for_testing_other_months = 3;
#								for (my $month_error_margin = 1; $month_error_margin < 3; $month_error_margin++) {
#									my $dt_test_date = $dt_consensus->clone();
#									$dt_test_date->subtract( months => $month_error_margin );
#									my $test_diff = $dt_test_date->delta_days($dt_current_date)->delta_days();
#									if ($test_diff <= $days_acceptable_diff_for_testing_other_months) {
#										print "Page ",$page->get_zooniverse_id(),". $consensus_date_friendly is probably $month_error_margin years out. Current date is $current_date->{friendly}\n";
#										$consensus_annotation->set_note($dt_test_date->day()." ".$dt_test_date->month_abbr()." ".$dt_test_date->year());
#										last;
#									}
#									$dt_test_date = $dt_consensus->clone();
#									$dt_test_date->add( months => $month_error_margin );
#									$test_diff = $dt_test_date->delta_days($dt_current_date)->delta_days();
#									if ($test_diff <= $days_acceptable_diff_for_testing_other_months) {
#										print "Page ",$page->get_zooniverse_id(),". $consensus_date_friendly is probably $month_error_margin years out. Current date is $current_date->{friendly}\n";
#										$consensus_annotation->set_note($dt_test_date->day()." ".$dt_test_date->month_abbr()." ".$dt_test_date->year());
#										last;
#									}
#								}
							}
						}
						else {
							# $consensus_date_friendly isn't later than our $current_year
							# If it is more than $acceptable_date_jump different, test for the wrong year.
							# test if the year is just wrong by trying the same date with different years
							if ($diff_days > $acceptable_date_jump) {
								my $year_length_in_days = 365;
								for (my $year_error_margin = 1; $year_error_margin < 5; $year_error_margin++) {
									my $test_min = $year_length_in_days * $year_error_margin - $acceptable_date_jump;
									my $test_max = $year_length_in_days * $year_error_margin + $acceptable_date_jump;
									if ($diff_days > $test_min && $diff_days < $test_max) {
										print "Page ",$page->get_zooniverse_id(),". $consensus_date_friendly is probably $year_error_margin years out (most recent date was $current_date->{friendly})\n" if $debug > 2;
										# then use DateTime math to add the appropriate number of years.
										$dt_consensus->add( years => $year_error_margin );
										$consensus_annotation->set_note($dt_consensus->day()." ".$dt_consensus->month_abbr()." ".$dt_consensus->year());
										my $error = {
											'type'		=> 'cluster_error; incorrect_year_fixed',
											'detail'	=> 'the diaryDate value \''.$consensus_date_friendly.'\' for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' was fixed by changing the year to make it closer to the previously found date, \''.$current_date->{friendly}.'\'',
										};
										$self->data_error($error);
										last;
									}
								}
								my $diff_days = $dt_consensus->delta_days($dt_current_date)->delta_days();
								if ($diff_days > $acceptable_date_jump) {
									print "Page ",$page->get_zooniverse_id(),". Most recent date was $current_date->{friendly}. The next diaryDate cluster is $consensus_date_friendly. The date difference of $diff_days is greater than the one we determined to be acceptable ($acceptable_date_jump). What do we do here?\n" if $debug > 2;
									undef;
									next; # skip this cluster.
								}
							}
							else {
								# $consensus_date has jumped into the past, but not by as much as a year
								undef;
							}
						}
					}
					
					if (defined($page_date_lookup->{$date_y_coord})) {
						# we already have a date for this page number and row.
						if ($date_y_coord == 0 && ref($page_date_lookup->{$date_y_coord}) eq "HASH" && !defined($page_date_lookup->{$date_y_coord}{cluster})) {
							# if this is row 0, we may have an annotation brought over from the previous page
							# If we have an explicit annotation for row 0, we should replace the date that was 
							# brought across from the previous page as it isn't needed.
							# It should be easy to tell the date that was brought across as it doesn't
							# relate to a cluster.
							$page_date_lookup->{$date_y_coord}{friendly} = $consensus_date_friendly;
							$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($consensus_date_friendly);
							$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
						}
						elsif (ref($page_date_lookup->{$date_y_coord}) eq 'HASH' 
							&& $page_date_lookup->{$date_y_coord}{friendly} ne $consensus_date_friendly) {
							my $existing_sortable = $page_date_lookup->{$date_y_coord}{sortable};
							my $new_sortable = get_sortable_date($consensus_date_friendly);
							if ($existing_sortable > $new_sortable) {
								$page_date_lookup->{$date_y_coord} = {
									'friendly'	=> $consensus_date_friendly,
									'sortable'	=> $new_sortable,
									'cluster'	=> $cluster,
								};
							}
						}
						elsif (ref($page_date_lookup->{$date_y_coord}) eq 'ARRAY') {
							# Original Logic: we have at least three dates for this row. Add the new date to the existing array for dealing with later
							# Suggested new logic: Of the two dates, use the one that is closer to the recently used date (this may be a superfluous block)
							my $date_already_recorded_for_this_row = 0;
							foreach my $recorded_date (@{$page_date_lookup->{$date_y_coord}}) {
								if ($recorded_date->{friendly} eq $consensus_annotation->get_string_value()) {
									$date_already_recorded_for_this_row = 1;
									last;
								}
							}
							if (!$date_already_recorded_for_this_row) {
								push @{$page_date_lookup->{$date_y_coord}}, {'friendly' => $consensus_annotation->get_string_value(), 'sortable' => get_sortable_date($consensus_annotation->get_string_value()), 'cluster' => $cluster};
							}
						}
						else {
							undef; # are these always a situation where the new annotation for the row matches the existing?
						}
					}
					else {
#						$page_date_lookup->{$date_y_coord}{friendly} = $selected_value;
#						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($selected_value);
						if (!defined($consensus_annotation->get_string_value())) {
							undef;
						}
						$page_date_lookup->{$date_y_coord}{friendly} = $consensus_annotation->get_string_value();
						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($page_date_lookup->{$date_y_coord}{friendly});
						$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
						# ^ circular reference?
						$current_date->{friendly} = $page_date_lookup->{$date_y_coord}{friendly};
						$current_date->{sortable} = $page_date_lookup->{$date_y_coord}{sortable};
						$dt_current_date = $date_parser->parse_datetime($current_date->{friendly});						
					}
				}
				elsif ($cluster->count_annotations() < 2) {
					# There was no consensus for this cluster, but with less than 2 annotations, that's understandable
					next;
				}
				else {
					# this diaryDate cluster has no consensus annotation (and not for lack of annotations)
					undef;
				}
			} # end looping through clusters
			
#			my @rows = sort {$a <=> $b} keys %$page_date_lookup;
#			# resolve any instances of multiple dates for a row here.
#			foreach my $row (@rows) {
#				if (ref($page_date_lookup->{$row}) eq 'ARRAY') {
#					# of the multiple dates we now have for this row, confirm they are later
#					# than the immediately preceeding row. If they are, select the earlier one
#					# on the basis that this case is often caused by an entry like "15th-30th Sep"
#					my $previous_row = _get_previous_date_row_number($page_date_lookup,$row,\@rows);
#					my $qualified_dates = [];
#					foreach my $user_contributed_date (@{$page_date_lookup->{$row}}) {
#						if (ref($user_contributed_date) ne "HASH" || ref($page_date_lookup->{$previous_row}) ne "HASH") {
#							undef;
#							next;
#						}
#						if ($user_contributed_date->{sortable} > $page_date_lookup->{$previous_row}{sortable}) {
#							push @$qualified_dates, $user_contributed_date;
#						}
#					}
#					if (@$qualified_dates > 1) {
#						# if more than one date meets the "later than the previous date" criteria
#						my @user_contributed_dates = sort {$a->{sortable} <=> $b->{sortable}} @$qualified_dates;
#						$page_date_lookup->{$row} = $user_contributed_dates[0];
#					}
#					elsif (@$qualified_dates == 1) {
#						# only one of the user contributed dates is later than a previous date
#						$page_date_lookup->{$row} = $qualified_dates->[0];
#					}
#					else {
#						# none of the dates qualified - delete this $page_date_lookup row.
#						delete $page_date_lookup->{$row};
#						@rows = sort {$a <=> $b} keys %$page_date_lookup;
#					}
#				}
#			}
#			# repopulate the array of rows in case it has changed
#			@rows = sort {$a <=> $b} keys %$page_date_lookup;
#			# get the highest row-numbered date on the page, this will be the first date on the 
#			# next page
#			my $highest_row_number = $rows[-1];
#			$current_date->{friendly} = $page_date_lookup->{$highest_row_number}{friendly};
#			$current_date->{sortable} = $page_date_lookup->{$highest_row_number}{sortable};
#			$dt_current_date = $date_parser->parse_datetime($current_date->{friendly});
		}
	}
}

sub create_date_lookup_old {
	my ($self) = @_;
	# TODO: This sub runs after the establish_consensus() subroutine, but some diaryDate clusters may not have reached consensus
	# (I've seen examples where there is disagreement about the year for a page where the author neglected to include the year in
	# the dates.)
	# First, get the start date of the diary from the TNA Discovery Catalogue metadata
	my $start_date = $self->get_start_date();
	my $current_date = $start_date->day()." ".$start_date->month_abbr()." ".$start_date->year();
	if ($current_date !~ /^\d{1,2} [a-z]{3} \d{4}/i) {
		undef;
	}
	my $current_sortable_date = get_sortable_date($current_date);

	if (!defined $self->{_pages}) {
		$self->load_pages();
	}

	# iterate through each page by ascending page number. Prepare the object hash reference, $date_lookup for content.
	# It will be keyed by page number then y-coordinate, later allowing a date lookup given those two parameters 
	foreach my $page (@{$self->{_pages}}) {
		my $page_num = $page->get_page_num();
		print "create_date_lookup, page $page_num\n" if $debug > 2;
		$date_lookup->{$page_num} = {};
		my $page_date_lookup = $date_lookup->{$page_num}; # the section of the $date_lookup hash referencing the current page
		
		# Before we process any annotations for the current page, set the date lookup for y-coord 0 to the $current_date value
		# For the first page of the diary, this is set to the StartDate field from the TNA Discovery Catalogue. For subsequent pages,
		# it is set to the last known date from the previous page
		$page_date_lookup->{0}{friendly} = $current_date;
		$page_date_lookup->{0}{sortable} = $current_sortable_date;
		
		# If there are no further dates on the page, this will be the only date lookup for the page. If there are other diaryDate
		# type annotations, iterate through them now, ascending sequentially by y-coordinate
		if (defined $page->{_clusters}{diaryDate}) {
			# The rest of the subroutine proceeds as follows for each cluster:
			# Is there a consensus_annotation?
			#  Y:	Does it have multiple possible values?
			#		 Y:	Find the previous date row on this page with a single date
			#			TODO: This should go back beyond page boundaries if necessary
			#			Select whichever of the possible dates is the closest later date than the previous known
			#			If a date is not selected, skip this cluster
			# Do we already have a date for this row?
			#  Y:	If it's row 0, replace the date we inferred from the previous page
			#		If it's another row, and different from the new row date we found, make an array to sort out later.
			#		If it's already an array, add this date to the list
			#  N:	Add the new date as the value for this row.
			
			foreach my $cluster (sort { $a->{median_centroid}[1] <=> $b->{median_centroid}[1] } @{$page->{_clusters}{diaryDate}}) {
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					# There was consensus for the date at this cluster
					my $date_y_coord = ($cluster->get_median_centroid())->[1];

					if (ref($consensus_annotation->get_note()) eq 'ARRAY') {
						# we have multiple possible dates for this cluster. Check the difference
						# from the last known date, and use the closest.
						my $smallest_distance_from_last_known_good;
						my $selected_value;
						my $last_known_good_row = _get_previous_date_row_number($page_date_lookup,$date_y_coord);
						while (ref($page_date_lookup->{$last_known_good_row}) ne 'HASH') {
							$last_known_good_row = _get_previous_date_row_number($page_date_lookup,$last_known_good_row);
						}
						foreach my $possible_value (@{$consensus_annotation->get_note()}) {
							my $sortable_date = get_sortable_date($possible_value);
							my $distance_from_last_known_good = $sortable_date - $page_date_lookup->{$last_known_good_row}{sortable};
							if ($distance_from_last_known_good >= 0 && (!defined($smallest_distance_from_last_known_good) || $distance_from_last_known_good < $smallest_distance_from_last_known_good)) {
								$selected_value = $possible_value;
								$smallest_distance_from_last_known_good = $distance_from_last_known_good;
							}
						}
						if (defined $selected_value) {
							$consensus_annotation->set_note($selected_value);
							my $error = {
								'type'		=> 'cluster_error; disputed_value_tie_resolved',
								'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed. Resolved by reference to undisputed neighbouring clusters',
							};
							$self->data_error($error);
						}
						else {
							my $error = {
								'type'		=> 'cluster_error; disputed_value_tie_unresolved',
								'detail'	=> 'the diaryDate value for the cluster at '.$cluster->{median_centroid}[0].','.$cluster->{median_centroid}[1].' is disputed with insufficient supporting data to resolve',
							};
							$self->data_error($error);
							undef;
							next; # skip this cluster
						}
					}
					
					if (defined($page_date_lookup->{$date_y_coord})) {
						# we already have a date for this page number and row.
						if ($date_y_coord == 0 && ref($page_date_lookup->{$date_y_coord}) eq "HASH" && !defined($page_date_lookup->{$date_y_coord}{cluster})) {
							# if this is row 0, we may have an annotation brought over from the previous page
							# If we have an explicit annotation for row 0, we should replace the date that was 
							# brought across from the previous page as it isn't needed.
							# It should be easy to tell the date that was brought across as it doesn't
							# relate to a cluster.
							$page_date_lookup->{$date_y_coord}{friendly} = $consensus_annotation->get_string_value();
							$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($page_date_lookup->{$date_y_coord}{friendly});
							$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
						}
						elsif (ref($page_date_lookup->{$date_y_coord}) eq 'HASH' 
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
							my $date_already_recorded_for_this_row = 0;
							foreach my $recorded_date (@{$page_date_lookup->{$date_y_coord}}) {
								if ($recorded_date->{friendly} eq $consensus_annotation->get_string_value()) {
									$date_already_recorded_for_this_row = 1;
									last;
								}
							}
							if (!$date_already_recorded_for_this_row) {
								push @{$page_date_lookup->{$date_y_coord}}, {'friendly' => $consensus_annotation->get_string_value(), 'sortable' => get_sortable_date($consensus_annotation->get_string_value()), 'cluster' => $cluster};
							}
						}
					}
					else {
#						$page_date_lookup->{$date_y_coord}{friendly} = $selected_value;
#						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($selected_value);
						if (!defined($consensus_annotation->get_string_value())) {
							undef;
						}
						$page_date_lookup->{$date_y_coord}{friendly} = $consensus_annotation->get_string_value();
						$page_date_lookup->{$date_y_coord}{sortable} = get_sortable_date($page_date_lookup->{$date_y_coord}{friendly});
						$page_date_lookup->{$date_y_coord}{cluster} = $cluster;
						# ^ circular reference?
					}
				}
				elsif ($cluster->count_annotations() < 2) {
					# There was no consensus for this cluster, but with less than 2 annotations, that's understandable
				}
				else {
					# this diaryDate cluster has no consensus annotation (and not for lack of annotations)
					undef;
				}
			} # end looping through clusters
			
			my @rows = sort {$a <=> $b} keys %$page_date_lookup;
			# resolve any instances of multiple dates for a row here.
			foreach my $row (@rows) {
				if (ref($page_date_lookup->{$row}) eq 'ARRAY') {
					# of the multiple dates we now have for this row, confirm they are later
					# than the immediately preceeding row. If they are, select the earlier one
					# on the basis that this case is often caused by an entry like "15th-30th Sep"
					my $previous_row = _get_previous_date_row_number($page_date_lookup,$row,\@rows);
					my $qualified_dates = [];
					foreach my $user_contributed_date (@{$page_date_lookup->{$row}}) {
						if (ref($user_contributed_date) ne "HASH" || ref($page_date_lookup->{$previous_row}) ne "HASH") {
							undef;
							next;
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
						@rows = sort {$a <=> $b} keys %$page_date_lookup;
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

sub get_surrounding_dates_for {
	my ($self, $page, $row) = @_;
	if (keys %$date_lookup < 1) {
		$self->create_date_lookup();
	}
	my $date_lookup_slice = @{$date_lookup}{$page-1,$page,$page+1};
	undef;
}

sub create_place_lookup {
	my ($self) = @_;
	my $current_place = ""; # $current_place keeps track of the most recently tagged placename from the previous page

	if (!defined $self->{_pages}) {
		$self->load_pages();
	}
	#iterate through each page by ascending page number
	foreach my $page (@{$self->{_pages}}) {
		my $page_num = $page->get_page_num();
		$place_lookup->{$page_num} = {};
		my $page_place_lookup = $place_lookup->{$page_num}; # the section of the $place_lookup hash referencing the current page
		$page_place_lookup->{0} = $current_place;
		if (defined $page->{_clusters}{place}) {
			# There are places tagged on this page - iterate through them in y-axis order
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
						if (ref($page_place_lookup->{$place_y_coord}) eq 'OWD::ConsensusAnnotation') {
							# if the row has a single place so far, and it's different from the one
							# we've just found, convert this value to an array and deal with it when 
							# the rest of the places for this page have been processed
							my $value1 = $page_place_lookup->{$place_y_coord};
							$page_place_lookup->{$place_y_coord} = 
								[$value1, $consensus_annotation];
							next;
						}
						elsif (ref($page_place_lookup->{$place_y_coord}) eq 'ARRAY') {
							# we have at least three dates for this row.
							# Add the new date to the existing array for dealing with later
							undef;
							push @{$page_place_lookup->{$place_y_coord}}, $consensus_annotation;
						}
					}
					else {
						$page_place_lookup->{$place_y_coord} = $consensus_annotation;
					}
				}
				else {
					# no consensus annotation for this place cluster
					undef;
				}
			}
			my @rows = sort {$a <=> $b} keys %$page_place_lookup;
			# repopulate the array of rows in case it has changed
			@rows = sort {$a <=> $b} keys %$page_place_lookup;
			# get the highest row-numbered place on the page, this will be the first place on the 
			# next page
			my $highest_row_number = $rows[-1];
			$current_place = $page_place_lookup->{$highest_row_number};
		}
	}
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
		if (!defined $doctype) {
			undef;
		}
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
	print $fh "#Unit\tPageNum\tPageID\tPageType\tDate\tPlace\tAnnotationType\tAnnotationValue\n";
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
			print $fh "$title\t$page_num\t$zooniverse_id\t$doctype\t$current_date\t$place\thashtags\t",join(",",@hashtags),"\n" if (@hashtags > 0);
		}
		my $chrono_clusters;
		my $date_boundaries;
		foreach my $type (keys %{$page->{_clusters}}) {
			if ($type eq 'diaryDate') {
				foreach my $cluster (@{$page->{_clusters}{$type}}) {
					if (ref($cluster) eq 'OWD::Cluster' && defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
						$date_boundaries->{$consensus_annotation->get_string_value()} = ${$consensus_annotation->get_coordinates()}[1];
					}
					else {
						undef;
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
						print $fh "$title\t$page_num\t$zooniverse_id\t$doctype\t$current_date\t$place\t$type\t",$consensus_annotation->get_string_value(),"\n";
					}
				}
			}
		}
	}
}

sub print_place_person_report {
	# for each page, get the person and place clusters and arrange them in hash $chrono_clusters keyed by ascending y coordinate
	# then for each ascending y_coordinate:
	# if we have a place cluster and it's a different place than we had the unit located already, add a new location record to the report
	# if we have a person, log them in the current location
	# sometimes a place or person is tagged more than once in a day. There is no point listing these multiple times per day (unless it is
	# a person mentioned in a different context than previously) so keep track of what we've logged per day.
	my ($self, $fh) = @_;
	print $fh "#Unit\tPageNum\tPageID\tPageType\tDate\tPlace\tPeople\n";
	my $current_place = '';
	foreach my $page (@{$self->{_pages}}) {
		my $title 		= $self->{_group_data}{name};
		my $doctype		= $page->get_doctype();
		my $page_num	= $page->get_page_num();
		my $zooniverse_id	= $page->get_zooniverse_id();
		my $date = ${get_date_for($page_num, 0)}{friendly};
		my $place = [$current_place];
		my $logged_for;
		#my $place = get_place_for($page_num, 0);
		#if (ref $place ne 'OWD::ConsensusAnnotation') {
		#	undef;
		#}
		my $current_date;
		if (!defined($current_date) || $date ne $current_date) {
			$current_date = $date;
		}
		my $chrono_clusters;
		foreach my $type (qw/place person/) {
			if (defined $page->{_clusters}{$type}) {
				foreach my $cluster (@{$page->{_clusters}{$type}}) {
					push @{$chrono_clusters->{$cluster->{median_centroid}[1]}{$type}}, $cluster;
				}
			}
		}
		foreach my $y_coord (sort keys %$chrono_clusters) {
			my $person_to_log = 0;
			my $person;
			$date = ${get_date_for($page_num, $y_coord)}{friendly};
			if (defined $chrono_clusters->{$y_coord}{place}) {
				foreach my $place_cluster (@{$chrono_clusters->{$y_coord}{place}}) {
					if (my $consensus_place = $place_cluster->get_consensus_annotation()) {
						push @$place, $consensus_place->get_string_value();
					}
					else {
						undef;
					}
				}
			}
			if (defined $chrono_clusters->{$y_coord}{person}) {
				foreach my $person_cluster (@{$chrono_clusters->{$y_coord}{person}}) {
					if (my $consensus_person = $person_cluster->get_consensus_annotation()) {
						push @$person, $consensus_person->get_string_value();
						print $fh "$title\t$page_num\t$zooniverse_id\t$doctype\t$date\t$place\t$person\n";
						print "$title\t$page_num\t$zooniverse_id\t$doctype\t$date\t$place\t$person\n";
						$person_to_log =  1;
					}
				}
			}
			if (@$place > 0) {
				# we have a place mentioned this y_coord
				# have we logged it for this day yet?
				foreach my $place_name (@$place) {
					if ($place_name ne $current_place
						&& defined $logged_for->{$date}
						&& defined $logged_for->{$date}{places}
						&& !defined $logged_for->{$date}{places}{$place_name}) {
							# new place that we haven't yet logged for this day
							# is there a person record?
							if (!$person_to_log) {
							}
					}
				}
			}
			if ($person_to_log && $place ne $current_place) {
				print $fh "$title\t$page_num\t$zooniverse_id\t$doctype\t$date\t$place\t\n";
				$current_place = $place;
			}
		}
	}
}

sub print_place_report {
	my ($self, $fh) = @_;
	print $fh "#Unit\tPageNum\tPageID\tPageType\tDate\tPlace\n";
	foreach my $page (@{$self->{_pages}}) {
		my $title 		= $self->{_group_data}{name};
		my $doctype		= $page->get_doctype();
		my $page_num	= $page->get_page_num();
		my $zooniverse_id	= $page->get_zooniverse_id();
		my $date = ${get_date_for($page_num, 0)}{friendly};
		my $current_date;
		if (!defined($current_date) || $date ne $current_date) {
			$current_date = $date;
		}
		my $logged_for;
		my $chrono_clusters;
		if (defined $page->{_clusters}{place}) {
			foreach my $cluster (@{$page->{_clusters}{place}}) {
				push @{$chrono_clusters->{$cluster->{median_centroid}[1]}}, $cluster;
			}
		}
		foreach my $y_coord (sort keys %$chrono_clusters) {
			my $place = [];
			$date = ${get_date_for($page_num, $y_coord)}{friendly};
			foreach my $place_cluster (@{$chrono_clusters->{$y_coord}}) {
				if (my $consensus_place = $place_cluster->get_consensus_annotation()) {
					push @$place, $consensus_place->get_string_value();
				}
				else {
					undef;
				}
			}
			if (@$place > 0) {
				# we only want to log place names that haven't been mentioned yet today
				foreach my $place_name (@$place) {
					next if $place_name eq '';
					if (!defined $logged_for->{$date}
						|| !defined $logged_for->{$date}{$place_name}) {
						# new place that we haven't yet logged for this day
						print $fh "$title\t$page_num\t$zooniverse_id\t$doctype\t$date\t$place_name\n";
						$logged_for->{$date}{$place_name} = 1;
					}
				}
			}
		}
	}
}

sub print_activities_report {
	my ($self, $fh) = @_;
	my $title 		= $self->{_group_data}{name};
	my $diary_id	= $self->get_zooniverse_id();
	my $tags_by_date;
	my $all_tags_by_date;
	foreach my $page (@{$self->{_pages}}) {
		my $page_num		= $page->get_page_num();
		my $doctype			= $page->get_doctype(); 
		my $zooniverse_id	= $page->get_zooniverse_id();
		if (defined (my $hashtags = $page->get_hashtags())) {
			my @hashtags = keys %$hashtags;
		}
		foreach my $type (keys %{$page->{_clusters}}) {
			next if $type eq 'diaryDate' or $type eq 'doctype'; # we've used these to create our date_lookup function already
			foreach my $cluster (@{$page->{_clusters}{$type}}) {
				next if ($cluster->get_type() ne 'activity' && $cluster->get_type() ne 'domestic');
				my $date = get_date_for($page_num, $cluster->{median_centroid}[1]);
				my $place = get_place_for($page_num, $cluster->{median_centroid}[1]);
				$tags_by_date->{$date->{sortable}}{places}{$place}++;
				$tags_by_date->{$date->{sortable}}{date} = $date->{friendly};
				$tags_by_date->{$date->{sortable}}{pages}{$page_num} = {'doctype' => $doctype, 'zooniverse_id' => $zooniverse_id, };
				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
					my $tag = $consensus_annotation->get_type().":".$consensus_annotation->get_note();
					$tags_by_date->{$date->{sortable}}{tags}{$tag}++;
				}
				else {
					undef;
				}
				foreach my $annotation (@{$cluster->{_annotations}}) {
					my $tag_date = get_date_for($page_num, $annotation->{_annotation_data}{coords}[1]);
					my $tag_place = get_place_for($page_num, $annotation->{_annotation_data}{coords}[1]);
					my $tag = $annotation->get_type().":".$annotation->get_note();
					$all_tags_by_date->{$tag_date->{sortable}}{places}{$place}++;
					$all_tags_by_date->{$tag_date->{sortable}}{date} = $date->{friendly};
					$all_tags_by_date->{$tag_date->{sortable}}{pages}{$page_num} = {'doctype' => $doctype, 'zooniverse_id' => $zooniverse_id, };
					$all_tags_by_date->{$tag_date->{sortable}}{tags}{$tag}++;
				}
				undef;
			}
		}
		undef;
	}
	open my $ofh, ">", "output-activities/$diary_id.tsv";
	my @activities = (
						"activity:achieved",
						"activity:attack",
						"activity:casualty",
						"activity:clearing",
						"activity:construction",
						"activity:enemy_activity",
						"activity:fire",
						"activity:line",
						"activity:movement",
						"activity:other",
						"activity:quiet",
						"activity:raid",
						"activity:reconnoitered",
						"activity:repair",
						"activity:reserve",
						"activity:resting",
						"activity:resupplying",
						"activity:strength",
						"activity:support",
						"activity:training",
						"activity:withdraw",
						"activity:working",
						);
	my @domestic = (
						"domestic:accommodation",
						"domestic:discipline",
						"domestic:hygiene",
						"domestic:inspections",
						"domestic:medical",
						"domestic:other",
						"domestic:parades",
						"domestic:rations",
						"domestic:religion",
						"domestic:sport",
						"domestic:uniform",
	);
	print $ofh "#Unit\tDate\tPageNum\tPageID\tPlace1\tPlace2\tPlace3\t";
	print $ofh join("\t",@activities),"\t";
	print $ofh join("\t",@domestic);
	print $ofh "\n";
	foreach my $date (sort keys %$tags_by_date) {
		my @pages = sort {$a <=> $b} keys %{$tags_by_date->{$date}{pages}};
		my @page_ids = ();
		foreach my $page_num (@pages) {
			push @page_ids, $tags_by_date->{$date}{pages}{$page_num}{zooniverse_id};
		}
		my $friendly_date = $tags_by_date->{$date}{date};
		if (!defined $friendly_date) {
			print "$friendly_date is undefined\n";
			print Dumper $tags_by_date->{$date};
		}
		print $ofh "$title\t$friendly_date\t@pages\t@page_ids\t";
		my @places = sort keys %{$tags_by_date->{$date}{places}};
		for (my $i=0; $i<3; $i++) {
			if (defined $places[$i]) {
				print $ofh "$places[$i]\t";
			}
			else {
				print $ofh "\t";
			}
		}
		foreach my $tag_type (@activities) {
			if (defined($tags_by_date->{$date}{tags}{$tag_type})) {
				print $ofh "$tags_by_date->{$date}{tags}{$tag_type}\t";
			}
			else {
				print $ofh "\t";
			}
		}
		foreach my $tag_type (@domestic) {
			if (defined($tags_by_date->{$date}{tags}{$tag_type})) {
				print $ofh "$tags_by_date->{$date}{tags}{$tag_type}\t";
			}
			else {
				print $ofh "\t";
			}
		}
		print $ofh "\n";
	}
	close $ofh;
	undef $tags_by_date;
	open $ofh, ">", "output_all-tags/$diary_id.tsv";
	print $ofh "#Unit\tDate\tPageNum\tPageID\tPlace1\tPlace2\tPlace3\t";
	print $ofh join("\t",@activities),"\t";
	print $ofh join("\t",@domestic);
	print $ofh "\n";
	foreach my $date (sort keys %$all_tags_by_date) {
		my @pages = sort {$a <=> $b} keys %{$all_tags_by_date->{$date}{pages}};
		my @page_ids = ();
		foreach my $page_num (@pages) {
			push @page_ids, $all_tags_by_date->{$date}{pages}{$page_num}{zooniverse_id};
		}
		my $friendly_date = $all_tags_by_date->{$date}{date};
		if (!defined $friendly_date) {
			print "$friendly_date is undefined\n";
			print Dumper $tags_by_date;
			<STDIN>;
		}
		print $ofh "$title\t$friendly_date\t@pages\t@page_ids\t";
		my @places = keys %{$all_tags_by_date->{$date}{places}};
		if (@places > 3) {
			undef;
		}
		for (my $i=0; $i<3; $i++) {
			if (defined $places[$i]) {
				print $ofh "$places[$i]\t";
			}
			else {
				print $ofh "\t";
			}
		}
		foreach my $tag_type (@activities) {
			if (defined($all_tags_by_date->{$date}{tags}{$tag_type})) {
				print $ofh "$all_tags_by_date->{$date}{tags}{$tag_type}\t";
			}
			else {
				print $ofh "\t";
			}
		}
		foreach my $tag_type (@domestic) {
			if (defined($all_tags_by_date->{$date}{tags}{$tag_type})) {
				print $ofh "$all_tags_by_date->{$date}{tags}{$tag_type}\t";
			}
			else {
				print $ofh "\t";
			}
		}
		print $ofh "\n";
	}
	undef;
	close $ofh;
#	my $tags_by_date;
#	my $current_date;
#	foreach my $page (sort {$a <=> $b} keys %$chrono_clusters) {
#		foreach my $y_coord (sort {$a <=> $b} keys %{$chrono_clusters->{$page}}) {
#			my $date = get_date_for($page, $y_coord);
#			my $place = get_place_for($page, $y_coord);
#			if (!defined($current_date) || $date->{friendly} ne $current_date) {
#				$current_date = $date->{friendly};
#			}
#			if ($y_coord == 0) {
#				push @{$tags_by_date->{$date->{sortable}}{page}}, $page;
#				push @{$tags_by_date->{$date->{sortable}}{zooniverse_id}}, $chrono_clusters->{$page}{0}{meta}{zooniverse_id};
#			}
#			
#			foreach my $cluster (@{$chrono_clusters->{$page}{$y_coord}{clusters}}) {
#				if (defined(my $consensus_annotation = $cluster->get_consensus_annotation())) {
#					if ((my $type = $consensus_annotation->get_type()) ne 'diaryDate') {
#						push @{$tags_by_date->{$date->{sortable}}
#						#print $fh "$title\t$page\t$zooniverse_id\t$doctype\t$current_date\t$place\t$type\t",$consensus_annotation->get_string_value(),"\n";
#					}
#				}
#			}
#		}
#	}
}

sub resolve_uncertainty {
	my ($self) = @_;
	foreach my $page (@{$self->{_pages}}) {
		$page->resolve_uncertainty();
	}
}

sub fix_suspect_diaryDates {
	my ($self) = @_;
	# Look for likely errors like incorrect guesses at years where the original diary page did not mention the year
	# eg. keep a running tally of the most recently used date and if the dates on the next page jump backwards or forwards
	# approximately n*365 days, assume that the year needs to be corrected
	my $latest_date_from_previous_page;
	foreach my $page (@{$self->{_pages}}) {
		#my ($earliest_date_for_page,$latest_date_for_page,$median_date_for_page);
		my $latest_date_for_page;
		if (defined $page->{_clusters}{diaryDate}) {
			foreach my $cluster (@{$page->{_clusters}{diaryDate}}) {
				my $consensus_annotation = $cluster->get_consensus_annotation();
				if (defined $consensus_annotation) {
					my $sortable_date = get_sortable_date($consensus_annotation->get_note());
					if (!defined $latest_date_for_page || $sortable_date > $latest_date_for_page) {
						$latest_date_for_page = $sortable_date;
					}
					else {
						# step back in time?
						undef;
					}
				}
				else {
					# no consensus on date?
					undef;
					
				}
			}
		}
		else {
			$latest_date_for_page = $latest_date_from_previous_page;
		}
		if (!defined $latest_date_from_previous_page || $latest_date_for_page >= $latest_date_from_previous_page) {
			$latest_date_from_previous_page = $latest_date_for_page;
		}
		else {
			if (!defined $latest_date_for_page) {
				undef;
			}
			else {
				# if latest_date_for_page is approximately n*365 dsys earlier or later than
				# the previous page, work out how to fix it.
				
				undef;
			}
		}
		undef;
	}
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
	if ($date_string !~ /^\d{1,2} [a-z]{3} \d{4}$/i) {
		undef;
	}
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

=pod

=head1 NAME

OWD::Diary - a class representing a single War diary, analagous to a document from the war_diary_groups collection of the OWD Mongo database

=head1 VERSION

v0.1

=head1 SYNOPSIS

use OWD::Diary;

my $diary = OWD::Diary->new();	returns a Diary object with only the diary-level metadata loaded 

$diary->load_pages();		Loads the page metadata from the Mongo db into tbe diary object


=head1 DESCRIPTION

=head1 SUBROUTINES/METHODS

=head2 load_pages

	$diary->load_pages();
	
Loads the page metadata from the Mongo database into the diary object

=head2 load_classifications

	$diary->load_classifications
	
Loads the user classifications from the database

=cut

1;
