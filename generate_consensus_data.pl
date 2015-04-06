#!/users/steven/perl5/perlbrew/perls/perl-5.20.0/bin/perl
use strict;
use warnings;
use OWD::Processor;
use MongoDB;
use Data::Dumper;

my $debug = 1;
my $war_diary_server	= "localhost:27017";
my $war_diary_db_name	= "war_diary_2014-11-24";
my $war_diary_output_db	= "war_diary_export";
my $war_diary_logging_db = "war_diary_logging";

my $client 	= MongoDB::MongoClient->new(host => $war_diary_server);
my $db 		= $client->get_database($war_diary_db_name);
my $output_db	= $client->get_database($war_diary_output_db);
my $logging_db = $client->get_database($war_diary_logging_db);

my $owd = OWD::Processor->new();
$owd->set_database($db);
$owd->set_output_db($output_db);
$owd->set_logging_db($logging_db);

my $total_raw_tag_counts;
my $diary_count = 0;
my $diary_id = "GWD0000001";
#while (my $diary = $owd->get_diary()) {
my $diary = $owd->get_diary($diary_id);
{
	$diary_count++;
	$owd->get_logging_db()->get_collection('error')->remove({"diary.group_id" => "$diary_id"});
	print "$diary_count: ",$diary->get_docref()," (".$diary->get_zooniverse_id().")\n";
	my $num_pages_with_classifications = $diary->load_classifications();
	if ($diary->get_status() eq "complete") {
		$diary->strip_multiple_classifications_by_single_user();
		$diary->report_pages_with_insufficient_classifications();
		$diary->cluster_tags();
		$diary->establish_consensus();
		$diary->create_date_lookup();
		open my $text_report, ">", "text_report.txt";
		$diary->print_text_report($text_report);
		
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