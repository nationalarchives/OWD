#!/users/steven/perl5/perlbrew/perls/perl-5.20.0/bin/perl
use strict;
use warnings;
use OWD::Processor;
use MongoDB;
use Data::Dumper;

# db connection strings
my $debug = 1;
my $war_diary_server	= "localhost:27017";
my $war_diary_db_name	= "war_diary_2014-11-24";	# the raw source data
my $war_diary_output_db	= "war_diary_export";		# the exported consensus data
my $war_diary_logging_db = "war_diary_logging";		# the logging db, recording any errors in clustering and consensus finding
my $war_diary_tags		= "ouroboros_production";	# the raw Talk pages (for hash tags and page talk)
my $war_diary_confirmed	= "war_diary_confirmed";

my $client 		= MongoDB::MongoClient->new(host => $war_diary_server);
my $db 			= $client->get_database($war_diary_db_name);
my $output_db	= $client->get_database($war_diary_output_db);
my $logging_db 	= $client->get_database($war_diary_logging_db);
my $tag_db		= $client->get_database($war_diary_tags);
my $confirmed_db= $client->get_database($war_diary_confirmed);

my $owd = OWD::Processor->new();
$owd->set_database($db); # this sets up the main DB connection, but also loads Group (diary-level) data 
						 # into the Processor object
$owd->set_output_db($output_db);
$owd->set_logging_db($logging_db);
$owd->set_tags_db($tag_db);
$owd->set_confirmed_db($confirmed_db);

my $total_raw_tag_counts;
my $diary_count = 0;
my $diary_id = "GWD0000006";
# OWD::Processor::get_diary() fetches the requested diary (or iterates through all the diaries in the DB if
# called with an empty parameter list). It loads the diary data, then loads page data too
while (my $diary = $owd->get_diary())
#my $diary = $owd->get_diary($diary_id);
{
	$diary_count++;
	my $diary_id = $diary->get_zooniverse_id();
	print "Processing diary $diary_id\n";
	# clear down the log db for this diary ID
	$owd->get_logging_db()->get_collection('error')->remove({"diary.group_id" => "$diary_id"});
	$owd->get_logging_db()->get_collection('log')->remove({"diary.group_id" => "$diary_id"});
	print "$diary_count: ",$diary->get_docref()," (".$diary->get_zooniverse_id().")\n";
	print "Loading classifications\n";
	# OWD::Diary::load_classifications() iterates through each page of the current diary loading classifications
	my $num_pages_with_classifications = $diary->load_classifications();
	if ($diary->get_status() eq "complete") {
		$diary->strip_multiple_classifications_by_single_user();
		$diary->report_pages_with_insufficient_classifications();
		$diary->load_hashtags();
		print "Clustering tags\n";
		$diary->cluster_tags();
		print "Establishing consensus\n";
		$diary->establish_consensus();
		print "Creating date lookup\n";
		$diary->create_date_lookup();
		print "Creating place lookup\n";
		$diary->create_place_lookup();
		$diary->resolve_uncertainty();
		open my $text_report, ">", "output/$diary_id-text.txt";
		$diary->print_text_report($text_report);
		close $text_report;
		open my $tsv_report, ">", "output/$diary_id.tsv";
		$diary->print_tsv_report($tsv_report);
		close $tsv_report;
		$diary->publish_to_db();
		my $tag_types = {};
		my $diary_raw_tag_type_counts = $diary->get_raw_tag_type_counts();
		while (my ($type,$count) = each %$diary_raw_tag_type_counts) {
			$total_raw_tag_counts->{$type} += $count;
		}
	}

	$diary->DESTROY();
	undef $diary;
}

my $total_tags;
foreach my $val (values %$total_raw_tag_counts) {
	$total_tags += $val;
}

foreach my $key (reverse sort {$total_raw_tag_counts->{$a} <=> $total_raw_tag_counts->{$b}} keys %$total_raw_tag_counts) {
	print "$key\t",int( ( $total_raw_tag_counts->{$key} / $total_tags )*100), "% ($total_raw_tag_counts->{$key})\n";
}