package MintUtils;

use strict;

use base qw( Exporter );

use XML::Twig;
use Text::CSV;
use Data::Dumper;

our @EXPORT_OK = qw(read_csv write_csv read_mint_cfg);

=head NAME

MintUtils

=head DESCRIPTION

A couple of shared routines used in the Mint integration scrips

=head SYNOPSIS;

use MintUtils qw(write_csv read_csv read_mint_config);

=head METHODS

=over 4

=item read_csv(file => $file, config => $config)

Reads a CSV file and returns a hashref of records. The 'config' param
is a hashref with two members, 'fields' (an arrayref of column names)
and 'id' (which of the columns in 'fields' to use as an unique key).
The config values are read from the XML config file.

It assumes that the first line is headers.

=cut

sub read_csv {
    my %params = @_;

    my $dir = $params{dir};
    my $file = $params{file};
    my $config = $params{config};
    my $query = $params{query};

    my $fconf = $config->{$query}{files}{$file} || do {
	die("Query/file $query/$file not found, check the config file.\n");
    };


    my @fields = @{$fconf->{fields}};
    my $filename = $fconf->{filename};

    my $csv = Text::CSV->new();

    my $path = join('/', $dir, $filename);

    my $id = $config->{$query}{id};

    my $records = {};

    my $csv = Text::CSV->new();

    open my $fh, "<:encoding(utf8)", $path || die("$path: $!");

    my $header = $csv->getline($fh);

    while( my $row = $csv->getline($fh) ) {
	my $record = {};
	for my $field ( @fields ) {
	    $record->{$field} = shift @$row;
	}
	$records->{$record->{$id}} = $record;
    }

    close $fh;

    return $records;
}


=item write_csv(file => $file, config => $config, records => $records)

Generic sub to write out a CSV file.  Config hashref is the same as for
read_csv.

=cut

sub write_csv {
    my %params = @_;

    my $dir = $params{dir};
    my $file = $params{file};
    my $config = $params{config};
    my $query = $params{query};
    my $records = $params{records};

    my $fconf = $config->{$query}{files}{$file} || do {
	die("Query/file $query/$file not found, check the config file.\n");
    };

    my @fields = @{$fconf->{fields}};
    my $filename = $fconf->{filename};

    die("No filename") unless $filename;

    my $csv = Text::CSV->new();

    my $path = join('/', $dir, $filename);

    open my $fh, ">:encoding(utf8)", $path || die "Can't write $path: $!";


    $csv->print($fh, \@fields);
    print $fh "\n";

    for my $id ( sort keys %$records ) {
	my %record = %{$records->{$id}};

	for my $field ( @fields ) {
	    if( my $cv = $fconf->{convert}{$field} ) { 
		if( exists $cv->{$record{$field}} ) {
		    print "[$field] convert $record{$field} to $cv->{$record{$field}}\n";
		    $record{$field} = $cv->{$record{$field}};
	        }
	    }
	}
       
	my $row = [ map { $record{$_} } @fields ];
	$csv->print($fh, $row);
	print $fh "\n";
    }

    close $fh || die "Can't close $path: $!";
}

=item read_mint_cfg(file => $file)

Loads the (important bits of) the XML config file. This is the same as
used by the java code which downloads the raw CSV files from the Staff
Module DB, so we only have to configure the files once.

Uses XML::Twig and XPath-style matches.

Each query can have multiple files, each of which has its own filename
and list of fields.  This is because data munging needs us to do multiple
passes on some classes of record before we write out a CSV with the
correct number of columns for harvesting.

Config structure is:

{
    $query => {
	id => $id,
	files => {
	    $label => {
		filename => $filename,
		fields => [ $f1, $f2, ... ],
		convert => { $f1 => { ... conversion hash ... }
	    },
	    $label2 => {
		...
            }	
	}
    },

    $query2 => { ... },

    ...

	dirs => { hashref of directories }

=cut

sub read_mint_cfg {
    my %params = @_;

    my $file = $params{file};

    my $config = {};

    my $xt = XML::Twig->new(
	twig_handlers => {

	    'locations' => sub {
		for my $dir ( $_->children() ) {
		    my $tag = $dir->tag;
		    $config->{dirs}{$tag} = $dir->text;
		}
	    },

	    'query/infields/field[@unique_ID="1"]' => sub {
		my $query = $_->parent('query')->{att}{name};
		$config->{$query}{id} = $_->{att}{name};
	    },

	    'query/outfields' => sub {
		
		my $query = $_->parent('query')->{att}{name};
		my $name = $_->{att}{name};
		if( !$config->{$query}{files}{$name} ) {
		    $config->{$query}{files}{$name} = {};
		}
		my $fc = $config->{$query}{files}{$name};
		$fc->{filename} = $_->{att}{file};
		$fc->{fields} = [];

		for my $field ( $_->children() ) {
		    push @{$fc->{fields}}, $field->{att}{name}
		}
	    },

	    'outfields/field/convert' => sub {
		my $field = $_->parent()->{att}{name};
		my $name = $_->parent('outfields')->{att}{name};
		my $query = $_->parent('query')->{att}{name};
		my $from = $_->{att}{from};
		my $to = $_->{att}{to};
		print "[#] $query/$name/$field/$from => $to\n";
		$config->{$query}{files}{$name}{convert}{$field}{$from} = $to;
	    },

	    'landingPageURLs/faculty' => sub {
		my $code = $_->{att}{code};
		$config->{landingPageURLs}{$code} = $_->text;
	    }, 

	    'groupTidy/ignore' => sub {
		$config->{groupTidy}{ignore} = $_->text;
	    },

	    'prefix' => sub {
		$config->{groupTidy}{$_->{att}{old}} = $_->{att}{new};
	    }

	}
    );
    $xt->parsefile($file);

    return $config;
}


=back

=cut
