#!/users/steven/perl5/perlbrew/perls/perl-5.20.0/bin/perl
use strict;
use warnings;
use OWD::Processor;
use MongoDB;
use Data::Dumper;
use Log::Log4perl;

my $execution_time = time();

# set up log4perl logging
my $logfile_name = "generate_consensus_data-$execution_time.log";

my $conf = q(
        log4perl.logger                    = DEBUG, FileApp, ScreenApp
        log4perl.appender.FileApp          = Log::Log4perl::Appender::File
        log4perl.appender.FileApp.layout   = PatternLayout
        log4perl.appender.FileApp.layout.ConversionPattern = %d %p> %m (%l)%n
        log4perl.appender.ScreenApp		   = Log::Log4perl::Appender::Screen
        log4perl.appender.ScreenApp.layout = PatternLayout
        log4perl.appender.ScreenApp.layout.ConversionPattern = %d %p> %m (%l)%n
    );

$conf .= "log4perl.appender.FileApp.filename = $logfile_name";

Log::Log4perl->init( \$conf );
my $logger = Log::Log4perl->get_logger();
$logger->warn("Log started");

$logger->info("=================\nConfiguration");

# db connection strings
my $war_diary_server	= "localhost:27017";
my $war_diary_db_name	= "war_diary_2018-01-24";	# the raw source data
my $war_diary_output_db	= "war_diary_export";		# the exported consensus data
my $war_diary_logging_db = "war_diary_logging";		# the logging db, recording any errors in clustering and consensus finding
my $war_diary_tags		= "ouroboros_production";	# the raw Talk pages (for hash tags and page talk)
my $war_diary_confirmed	= "war_diary_confirmed";

$logger->info("war_diary_server = $war_diary_server\nwar_diary_db_name = $war_diary_db_name\nwar_diary_output_db = $war_diary_output_db\nwar_diary_logging_db = $war_diary_logging_db\nwar_diary_tags = $war_diary_tags\nwar_diary_confirmed = $war_diary_confirmed\n=================\n");

my $client 		= MongoDB::MongoClient->new(host => $war_diary_server,query_timeout => 50000);
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

#my %already_processed;
#my @file_list = glob("output/*.tsv");
#foreach my $file (@file_list) {
#	if ($file =~ /(GWD.+)\.tsv$/i) {
#		$already_processed{$1} = 1;
#	}
#}
my @diary_ids;
if (@ARGV) {
	$logger->debug("Script called with arguments: @ARGV");
	foreach my $diary_id (@ARGV) {
		$logger->trace("Process diary $diary_id");
		if ($diary_id =~ /^GWD/) {
			my $diary = $owd->get_diary($diary_id);
			if (ref $diary ne "OWD::Diary") {
				$logger->error("$diary_id is not of type OWD::Diary");
			}
			diary_tasks($diary);
		}
	}
}
else {
	$logger->info("Script called without arguments: processing diaries sequentially");
	while (my $diary = $owd->get_diary()) {
		diary_tasks($diary);
	}
}

sub diary_tasks {
	my ($diary) = @_;
	$diary_count++;
	my $diary_id = $diary->get_zooniverse_id();
	$logger->trace("diary_tasks called with diary $diary_id");
#	if ($already_processed{$diary_id}) {
#		print "Skipping $diary_id\n";
#		return;
#	}
	# clear down the log db for this diary ID
	$logger->trace("Clearing down logging database entries for this diary");
	$owd->get_logging_db()->get_collection('error')->remove({"diary.group_id" => "$diary_id"});
	$owd->get_logging_db()->get_collection('log')->remove({"diary.group_id" => "$diary_id"});
	$logger->info("$diary_count: ",$diary->get_docref()," (",$diary->get_zooniverse_id(),")");
	if ($diary->get_status() eq "complete") {
		$logger->debug("  diary has been completed by volunteers. Loading pages.");
		$diary->load_pages(); # loads all the per-page metadata into the Diary object
		$logger->debug("Loading classifications");
		# OWD::Diary::load_classifications() iterates through each page of the current diary loading classifications, tidying them up 
		# as necessary before creating a classification object
		my $num_pages_with_classifications = $diary->load_classifications();
		$diary->strip_multiple_classifications_by_single_user();
		$diary->report_pages_with_insufficient_classifications();
		$diary->load_hashtags();
		$logger->debug("Clustering tags");
		$diary->cluster_tags();
		$logger->debug("Establishing consensus");
		$diary->establish_consensus(); # establish_consensus includes create_place_lookup

		# create_place_lookup populates a hash keyed by page and y-coord listing the consensus places
		# Each row will have one or more place ConsensusAnnotations
		$logger->debug("Creating place lookup");
		$diary->create_place_lookup();
		$diary->resolve_uncertainty();
		#$diary->fix_suspect_diaryDates();
#		open my $text_report, ">", "output/$diary_id-text.txt";
#		$diary->print_text_report($text_report);
#		close $text_report;
#		open my $place_person_report, ">", "output/$diary_id-place-person.tsv";
#		$diary->print_place_person_report($place_person_report);
#		close $place_person_report;
#		$diary->rationalise_person_names();
		open my $person_report, ">", "output/$diary_id-person.tsv";
		$diary->print_person_report($person_report);
		close $person_report;
		open my $place_report, ">:utf8", "output/$diary_id-place.tsv";
		$diary->print_place_report($place_report);
		close $place_report;
#		open my $tsv_report, ">", "output/$diary_id.tsv";
#		$diary->print_tsv_report($tsv_report);
#		close $tsv_report;
		#$diary->print_activities_report();

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