package MintUtils;

use strict;

use base qw( Exporter );

use XML::Twig;
use Text::CSV;
use Data::Dumper;
use Log::Log4perl;

my $LOGGER = 'MintInt.MintUtils';

our @EXPORT_OK = qw(read_csv write_csv read_mint_cfg);

=head1 NAME

MintUtils

=head1 DESCRIPTION

A couple of shared routines used in the Mint integration scrips

=head1 SYNOPSIS

use MintUtils qw(write_csv read_csv read_mint_config);


=head1 GLOBALS

=head2 $CONFIG_VALID

Crude validation of config entries.  A tree of hashes matching the
config tree.  If the value for group/var is a hashref, match by
regexp:

{
	re => qr/THE PATTERN/,
	desc => "a description of the pattern for the user"
}

Otherwise the value just has to be defined and not empty (zero
is OK)

=cut



my $CONFIG_VALID = {
	locations => {
		working => 1,
		harvest => 1,
		logs => 1,
	},
	query => {
		People => 1,
		Groups => 1,
		Projects => 1
	},
	landingPageURLs => {
		faculties => 1,
		urlID => 1,
	},
	staffIDs => {
		cryptKey => {
			re => qr/^[0-9a-zA-Z]{20}$/,
			desc => 'a 20-digit hexadecimal number'
		},
		originalID => 1
	}
};
	
	

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

	my $log = Log::Log4perl->get_logger($LOGGER);

    my $fconf = $config->{query}{$query}{files}{$file} || do {
		die("Query/file $query/$file not found, check the config file.\n");
    };


    my @fields = @{$fconf->{fields}};
    my $filename = $fconf->{filename};

    my $csv = Text::CSV->new();

    my $path = join('/', $dir, $filename);

    my $id = $fconf->{id} || $config->{query}{$query}{id};

    my $records = {};

    my $csv = Text::CSV->new();

	$log->info("Reading CSV from $path");


    open my $fh, "<:encoding(utf8)", $path || die("$path: $!");

    my $header = $csv->getline($fh);

	$log->debug("ID field = $id");

    while( my $row = $csv->getline($fh) ) {
		my $record = {};
		for my $field ( @fields ) {
	    	$record->{$field} = shift @$row;
		}
		$records->{$record->{$id}} = $record;
		$log->trace("Read record $record->{$id}");
    }

    close $fh;

    return $records;
}


=item write_csv(file => $file, config => $config, records => $records)

Generic sub to write out a CSV file.  Config hashref is the same as for
read_csv.

Parameters:

=over 4

=item file - complete filepath

=item config - hashref, same as for read_csv

=item records - data: a hashref by unique ID of hashrefs by field names

=item sortby - either a ref to a sort function, or an arrayref of sorted IDs

=back

=cut

sub write_csv {
    my %params = @_;

    my $dir = $params{dir};
    my $file = $params{file};
    my $config = $params{config};
    my $query = $params{query};
    my $records = $params{records};
    my $sortby = $params{sortby};

	my $log = Log::Log4perl->get_logger($LOGGER);

    my $fconf = $config->{query}{$query}{files}{$file} || do {
		die("Query/file $query/$file not found, check the config file.\n");
    };

    my @fields = @{$fconf->{fields}};
    my $filename = $fconf->{filename};

    die("No filename") unless $filename;

    my $csv = Text::CSV->new();

    my $path = join('/', $dir, $filename);


	$log->info("Writing CSV to $path");

    open my $fh, ">:encoding(utf8)", $path || die "Can't write $path: $!";


    $csv->print($fh, \@fields);
    print $fh "\n";
    
    my @keys = ();
    
    if( $sortby ) {
    	if( ref($sortby) eq 'ARRAY' ) {
    		@keys = @$sortby;
    	} elsif( ref($sortby) eq 'CODE' ) {
    		@keys = sort $sortby keys %$records;
    	} else {
    		$log->error(
    			"'sortby' argument must be an arrayref or coderef"
    			);
    		die("Invalid argument");
    	}
    } else {
    	@keys = sort keys %$records;
    }

    for my $id ( @keys ) {
	my %record = %{$records->{$id}};

	for my $field ( @fields ) {
	    if( my $cv = $fconf->{convert}{$field} ) { 
			if( exists $cv->{$record{$field}} ) {
		    	$log->debug("[$field] convert $record{$field} to $cv->{$record{$field}}");
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

	locations => { hashref of directories }

	FIXME - document the rest of this properly in a separate place
	Also this is out of date now.


=cut

sub read_mint_cfg {
    my %params = @_;

    my $file = $params{file};

    my $config = {};

	my $log = Log::Log4perl->get_logger($LOGGER);

    my $xt = XML::Twig->new(
		twig_handlers => {

		    'locations' => sub {
				for my $dir ( $_->children() ) {
		    		my $tag = $dir->tag;
		    		$config->{locations}{$tag} = $dir->text;
				}
	    	},

	    	'query/infields/field[@unique_ID="1"]' => sub {
				my $query = $_->parent('query')->{att}{name};
				$config->{query}{$query}{id} = $_->{att}{name};
	    	},

	 	   'query/outfields' => sub {
		
				my $query = $_->parent('query')->{att}{name};
				my $name = $_->{att}{name};
				if( !$config->{query}{$query}{files}{$name} ) {
		    		$config->{query}{$query}{files}{$name} = {};
				}
				my $fc = $config->{query}{$query}{files}{$name};
				$fc->{filename} = $_->{att}{file};
				$fc->{fields} = [];

				for my $field ( $_->children() ) {
				    push @{$fc->{fields}}, $field->{att}{name};
				    if( $field->{att}{unique_ID} ) {
				    	$fc->{id} = $field->{att}{name};
				    }
				}
	    	},

	    	'outfields/field/convert' => sub {
				my $field = $_->parent()->{att}{name};
				my $name = $_->parent('outfields')->{att}{name};
				my $query = $_->parent('query')->{att}{name};
				my $from = $_->{att}{from};
				my $to = $_->{att}{to};
				$log->trace("Substitute $query/$name/$field/$from => $to");
				$config->{query}{$query}{files}{$name}{convert}{$field}{$from} = $to;
	    	},

		    'landingPageURLs/faculty' => sub {
				my $code = $_->{att}{code};
				$config->{landingPageURLs}{faculties}{$code} = $_->text;
	    	}, 

	    	'landingPageURLs' => sub {
	    		$config->{landingPageURLs}{urlID} = $_->{att}{urlID};
	    	},

		    'groupTidy/ignore' => sub {
				$config->{groupTidy}{ignore} = $_->text;
	    	},

	    	'prefix' => sub {
				$config->{groupTidy}{$_->{att}{old}} = $_->{att}{new};
	    	},
	    	
	    	'staffIDs/cryptKey' => sub {
	    		$config->{staffIDs}{cryptKey} = $_->text;
	    	},
	    	
	    	'staffIDs/originalID' => sub {
	    		$config->{staffIDs}{originalID} = $_->text;
	    	},

		}
    );
    
    $xt->parsefile($file);

	if( validate_config(config => $config, log => $log) ) {
		return $config;
	} else {
		return undef;
	}
}


=item validate_config(config => $config)

Validate the parts of the config file that the Perl scripts need.

This assumes that we won't need to validate anything in detail
below two levels (for eg People/files)

=cut

sub validate_config {
	my %params = @_;
	
	my $config = $params{config};
	my $log = $params{log};
	
	my $errors = 0;
	
	for my $group ( keys %$CONFIG_VALID ) {
		if( !$config->{$group} ){
			$log->error("Config group '$group' is missing.");
			$errors = 1;
			next;
		}
		for my $var ( keys %{$CONFIG_VALID->{$group}} ) {
			my $matcher = $CONFIG_VALID->{$group}{$var};
			if( ref($matcher) ) {
				my $value = $config->{$group}{$var};
				if( $value !~ m/$matcher->{re}/i ) {
					$log->error("Config value '$group/$var' ('$value') must be $matcher->{des}");
					$errors = 1;
				}
			} else {
				if( !exists $config->{$group}{$var} ) {
					$log->error("Config value '$group/$var' is missing.");
					$errors = 1;
				}
			}
		}
	}
	if( $errors ) {
		$log->trace(Dumper($config));
		$log->error("Invalid config, can't continue");
		return 0;
	}
	return 1;
}



=back




=cut
