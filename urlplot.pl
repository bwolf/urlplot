# $Id: urlplot.pl,v 1.3.2.28 2003/02/10 09:17:28 bwolf Exp $

use 5.006_001;
use strict;
use integer;

use Carp;
use Time::Local ();
use POSIX ();
use Fcntl qw(:DEFAULT :flock);
use File::Spec::Functions;
use File::Basename qw(dirname);
use DB_File;
use Data::Dumper;
use Safe;

use Digest::MD5 qw(md5_hex);

use Irssi 20021117.1611 qw(
	settings_add_str settings_add_int settings_add_bool
	settings_get_str settings_get_int settings_get_bool
	command_bind signal_add_last
);

BEGIN {
	eval q{use DBI 1.28};
	Irssi::active_win()->print('urlplot: DBI support ' . ($@ ? 'disabled' : 'enabled'),
		MSGLEVEL_CLIENTCRAP);
}

use vars qw($VERSION %IRSSI); # http://de.irssi.org/scripts/ doesn't handle 'our' correctly *SIGH*

($VERSION = '$Revision: 1.3.2.28 $') =~ s|\$revision:\s+([^\s]+)\s+\$|\1|i;

%IRSSI = (
	authors		=> 'Marcus Geiger',
	contact		=> 'bwolf-urlplot@antbear.org',
	name		=> 'urlplot',
	description	=> 'Sophisticated URL grabber with HTML generation and cmd execution (DB-support [DBI|FILE],CSV)',
	modules		=> 'Carp Time::Local POSIX Fcntl File::Spec::Functions File::Basename DB_File Data::Dumper Safe Digest::MD5 (DBI)',
	license		=> 'BSD',
	url		=> 'http://www.antbear.org',
	changed		=> '$Date: 2003/02/10 09:17:28 $',
);

# Convert UTC CVS 'Date' keyword to RFC822 date in local timezone
{
	$IRSSI{changed} =~ m!(\d+/\d+/\d+)\s+(\d+:\d+:\d)!;
	my $epoch = Time::Local::timegm(reverse(split /:/, $2), reverse(split /\//, $1));
	$IRSSI{changed} = POSIX::strftime('%a %b %d %H:%M:%S %Z %Y', localtime($epoch));
}

# ----------- [ Constants ] -----------------------------------------

use constant RE_URL_DATA		=> qr/[A-Za-z0-9\\:?%\.&@\$!~;,=#<>\/_\+-]+/i;
use constant BACKWARD_SEEK_BYTES	=> 130;
use constant LOG_FILE_MARKER		=> '<!-- bottom-line -->';
use constant TYPE_PUBMSG		=> 'pubmsg';
use constant TYPE_PRIVMSG		=> 'privmsg';
use constant TYPE_TOPIC			=> 'topic';

my %KNOWN_DBMS		= (mysql => 1, postgresql => 1);
my @URL_COMMANDS	= sort qw(
	list browse-index sync-topics html-recreate stats
	db-select-where db-create db-flush help help-setting
);

my @URL_SETTINGS	= sort qw(
url_print_to_active_win		url_list_default_limit
url_cache_max			url_html_logging
url_html_date_format		url_html_basedir
url_html_filename 		url_html_file_maxsize
url_html_file_reloadtime	url_html_logging_locked
url_html_logging_lockfile	url_html_channel_logging
url_html_channel_prefix		url_csv_logging
url_csv_separator		url_csv_logfile
url_csv_logging_locked		url_csv_logging_lockfile
url_policy_default		url_policy_channels
url_policy_nicks		url_db_mode
url_db_dbi_dsn			url_db_dbi_user
url_db_dbi_passwd		url_db_file_basedir
url_db_file_hash_filename	url_db_file_array_filename
url_db_file_locked		url_db_file_lockfile
url_browser_command_write	url_browser_command
url_browse_index_url
);

# ----------- [ Globals ] -------------------------------------------

my %opt;
my $deparse_sandbox;
my %db_abstraction = (
	DBI => {
		db_connect		=>	\&db_dbi_connect,
		db_disconnect		=>	\&db_dbi_disconnect,
		db_get_urls		=>	\&db_dbi_get_urls,
		db_get_nth_url		=>	\&db_dbi_get_nth_url,
		db_log_urls		=>	\&db_dbi_log_urls,
		db_filter_unknown_urls	=>	\&db_dbi_filter_unknown_urls,
		db_count_logged		=>	\&db_dbi_count_logged,
		db_resize		=>	\&db_dbi_resize,
		db_stats		=>	\&db_dbi_stats,
	},
	FILE => {
		db_connect		=>	\&db_file_connect,
		db_disconnect		=>	\&db_file_disconnect,
		db_get_urls		=>	\&db_file_get_urls,
		db_get_nth_url		=>	\&db_file_get_nth_url,
		db_log_urls		=>	\&db_file_log_urls,
		db_filter_unknown_urls	=>	\&db_file_filter_unknown_urls,
		db_count_logged		=>	\&db_file_count_logged,
		db_resize		=>	\&db_file_resize,
		db_stats		=>	\&db_file_stats,
	},
);

# ----------- [ Settings ] ------------------------------------------

sub register_settings() {
	settings_add_bool('misc', 'url_print_to_active_win'	=> 0);
	settings_add_int('misc',  'url_list_default_limit'	=> -1);

	settings_add_int('misc',  'url_cache_max'		=> 90);

	settings_add_bool('misc', 'url_html_logging'		=> 1);
	settings_add_str('misc',  'url_html_date_format'	=> '%Y:%m:%d - %H:%M:%S');
	settings_add_str('misc',  'url_html_basedir'		=> homedir('~/.irssi/urlplot/html/'));

	settings_add_str('misc',  'url_html_filename'		=> 'ircurls.html');
	settings_add_int('misc',  'url_html_file_maxsize'	=> 1024 * 30);
	settings_add_int('misc',  'url_html_file_reloadtime'	=> 120);

	settings_add_bool('misc', 'url_html_logging_locked'	=> 0);
	settings_add_str('misc',  'url_html_logging_lockfile'	=> homedir('~/.irssi/urlplot/html.lock'));

	settings_add_bool('misc', 'url_html_channel_logging'	=> 1);
	settings_add_str('misc',  'url_html_channel_prefix'	=> 'chan_');

	settings_add_bool('misc', 'url_csv_logging'		=> 0);
	settings_add_str('misc',  'url_csv_separator'		=> '|');
	settings_add_str('misc',  'url_csv_logfile'		=> homedir('~/.irssi/urlplot/log.csv'));

	settings_add_bool('misc', 'url_csv_logging_locked'	=> 0);
	settings_add_str('misc',  'url_csv_logging_lockfile'	=> homedir('~/.irssi/urlplot/log.csv.lock'));

	settings_add_str('misc',  'url_policy_default'		=> 'allow');
	settings_add_str('misc',  'url_policy_channels'		=> '');
	settings_add_str('misc',  'url_policy_nicks'		=> '');

	settings_add_str('misc',  'url_db_mode'			=> 'FILE');

	settings_add_str('misc',  'url_db_dbi_dsn'		=> 'dbi:Pg:dbname=url_db');
	settings_add_str('misc',  'url_db_dbi_user'		=> '');
	settings_add_str('misc',  'url_db_dbi_passwd'		=> '');

	settings_add_str('misc',  'url_db_file_basedir'		=> homedir('~/.irssi/urlplot/db_file'));
	settings_add_str('misc',  'url_db_file_hash_filename'	=> 'db_h');
	settings_add_str('misc',  'url_db_file_array_filename'	=> 'db_a');

	settings_add_bool('misc', 'url_db_file_locked'		=> 0);
	settings_add_str('misc',  'url_db_file_lockfile'	=> homedir('~/.irssi/urlplot/db_file.lock'));

	settings_add_bool('misc', 'url_browser_command_write',	=> 0);
	settings_add_str('misc',  'url_browser_command',	=>
		'mozilla -remote "openURL(__URL__)" > /dev/null 2>&1 || mozilla "__URL__"&');

	settings_add_str('misc',  'url_browse_index_url',	=>
		'file://' . homedir('~/.irssi/urlplot/html/ircurls.html'));
}

sub load_settings() {
	$opt{print_to_active_win}	= settings_get_bool('url_print_to_active_win');
	$opt{list_default_limit}	= settings_get_int('url_list_default_limit');

	$opt{cache_max}			= settings_get_int('url_cache_max');

	$opt{html_logging}		= settings_get_bool('url_html_logging');
	$opt{html_date_format}		= settings_get_str('url_html_date_format');
	$opt{html_basedir}		= settings_get_str('url_html_basedir');

	$opt{html_filename}		= settings_get_str('url_html_filename');
	$opt{html_file_maxsize}		= settings_get_int('url_html_file_maxsize');
	$opt{html_file_reloadtime}	= settings_get_int('url_html_file_reloadtime');

	$opt{html_logging_locked}	= settings_get_bool('url_html_logging_locked');
	$opt{html_logging_lockfile}	= settings_get_str('url_html_logging_lockfile');

	$opt{html_channel_logging}	= settings_get_bool('url_html_channel_logging');
	$opt{html_channel_prefix}	= settings_get_str('url_html_channel_prefix');

	$opt{csv_logging}		= settings_get_bool('url_csv_logging');
	$opt{csv_separator}	 	= settings_get_str('url_csv_separator');
	$opt{csv_logfile} 		= settings_get_str('url_csv_logfile');

	$opt{csv_logging_locked}	= settings_get_bool('url_csv_logging_locked');
	$opt{csv_logging_lockfile}	= settings_get_str('url_csv_logging_lockfile');

	for (lc settings_get_str('url_policy_default')) {
		unless ($_ eq 'allow' || $_ eq 'deny') {
			print_err("setting 'url_policy_default' can be either 'allow' or 'deny'");
			print_err("setting not accepted");
		} else {
			$opt{policy_default} = $_;
		}
	}

	$opt{policy_channels}		= settings_get_str('url_policy_channels');
	$opt{policy_nicks}		= settings_get_str('url_policy_nicks');

	for (uc settings_get_str('url_db_mode')) {
		unless ($_ eq 'FILE' || $_ eq 'DBI') {
			print_err("setting 'url_db_mode' can be either 'file' or 'dbi'");
			print_err("setting not accepted");
		} else {
			$opt{db_mode} = $_;
		}
	}

	$opt{db_dbi_dsn}		= settings_get_str('url_db_dbi_dsn');
	$opt{db_dbi_user}		= settings_get_str('url_db_dbi_user');
	$opt{db_dbi_passwd}		= settings_get_str('url_db_dbi_passwd');

	$opt{db_file_basedir}		= settings_get_str('url_db_file_basedir');
	$opt{db_file_hash_filename}	= settings_get_str('url_db_file_hash_filename');
	$opt{db_file_array_filename}	= settings_get_str('url_db_file_array_filename');

	$opt{db_file_locking}		= settings_get_bool('url_db_file_locked');
	$opt{db_file_lockfile}		= settings_get_str('url_db_file_lockfile');

	$opt{browser_command_write}	= settings_get_bool('url_browser_command_write');

	for (settings_get_str('url_browser_command')) {
		unless ($opt{browser_command_write} || /__URL__/) {
			print_err("setting 'url_browser_command' has to contain '__URL__'");
			print_err("setting not accepted");
		} else {
			$opt{browser_command} = $_;
		}
	}

	$opt{browse_index_url}		= settings_get_str('url_browse_index_url');
}

# ----------- [ Initialization ] ------------------------------------

register_settings();
load_settings();

command_bind('url' => \&command_url);
{
	no strict 'refs';
	command_bind("url -$_" => \&{"url_command_$_"}) 
		for @URL_COMMANDS;
}

signal_add_last('message public'	=> \&signal_public_message);
signal_add_last('message private'	=> \&signal_private_message);
signal_add_last('message topic'		=> \&signal_topic_message);
signal_add_last('channel joined'	=> \&signal_channel_joined);
signal_add_last('command set'		=> \&signal_set);
signal_add_last('complete word'		=> \&signal_complete);

print CLIENTCRAP "$IRSSI{name} $VERSION - $IRSSI{changed} loaded: %9/url -help%9 for help";

# ----------- [ Utilities ] -----------------------------------------

sub homedir($) {
	my ($filename) = @_;
	$filename =~
		s{ ^ ~ ( [^/]* ) }
		 { $1
		   ? (getpwnam($1))[7]
		   : ( $ENV{HOME} || $ENV{LOGDIR} || (getpwuid($>))[7])
		 }ex;
	return $filename;
}

sub print_err(@) {
	# Error printing (directly to the current window)
	my @args = @_;
	s/%/%%/g for @args;
	Irssi::print("urlplot: @args");
}

sub print_out(@) {
	# Normal printing (to the msg window)
	if ($opt{print_to_active_win}) {
		Irssi::active_win()->print("@_", MSGLEVEL_CLIENTCRAP);
	} else {
		Irssi::print("@_", MSGLEVEL_MSGS + MSGLEVEL_NOHILIGHT);
	}
}

sub scan_urls($) {
	my($text) = @_;
	my(@urls, $url);
	for (split /\s+/, $text) {
		chomp;
		s/^\s+//;
		s/\s+$//;
		next if /^$/;

		if (m<
			^
			(?=[whfni])			# lookahead for 'www|https?|ftps?|news|irc'
			(				# capture
				(?=[hfni:/])		# lookahead for 'http|ftp|news|irc' and '://'
				(?:			# don't capture
					(?:ht|f)	# 'ht' or 'f'
					tp		# 'tp'
					s?		# optionally 'https' or 'ftps'
					|		# or
					news		# 'news'
					|		# or
					irc		# 'irc'
				)
				://			# scheme separator
				@{[RE_URL_DATA]}	# url data
				|			# or something that starts with 'www' or 'ftp'
				(?=[wf])		# lookahead for 'www|ftp'
				(?:www|ftp)		# do it
				\.			# literal dot
				@{[RE_URL_DATA]}	# url data
			)
			>gix)
		{
			$url = $1;
			if ($url =~ /^www/) {
				$url = "http://$url";
			} elsif ($url =~ /^ftp/ && $url !~ m|^ftp://|) {
				$url = "ftp://$url";
			}
			push @urls, $url;
		}
	}
	return \@urls;
}

sub logging_permited($$) {
	my($nick, $channel) = @_;
	my @policy_channels = split /[,;: ]/, $opt{policy_channels};
	my @policy_nicks    = split /[,;: ]/, $opt{policy_nicks};

	s/^#// for @policy_channels;
	s/^#// =~ $channel if defined($channel);

	if ($opt{policy_default} eq 'deny') {		# -- logging must be explicitly permited
		if (defined($channel)) { for (@policy_channels) { return 1 if $_ eq $channel } }
		if (defined($nick))    { for (@policy_nicks)    { return 1 if $_ eq $nick    } }
		return 0;
	} elsif ($opt{policy_default} eq 'allow') {	# -- logging must be explicitly denied
		if (defined($channel)) { for (@policy_channels) { return 0 if $_ eq $channel } }
		if (defined($nick))    { for (@policy_nicks)    { return 0 if $_ eq $nick    } }
		return 1;
	} 
	return undef;
}

sub url_log {
	my($info, $rawtext) = @_;

	# $info =
	# {
	# 	type	=>	TYPE_TOPIC | TYPE_PRIVMSG | TYPE_PUBMSG,
	# 	nick	=>	STRING,
	# 	channel	=>	STRING,
	# 	chatnet =>      STRING,
	# }

	# get all urls out of the raw IRC text
	my $urls = scan_urls($rawtext);
	return unless @$urls > 0;

	# map each url to its coresponding MD5 hex digest
	my $url_map;
	%$url_map = map { $_ => md5_hex($_) } @$urls;

	my $dbh;
	eval {
		$dbh = db_connect();

		# get a list of urls that are not already in the
		# database
		$url_map = db_filter_unknown_urls($dbh, $url_map);
		return unless (keys(%$url_map)) > 0;

		db_log_urls($dbh, $info, $url_map);

		# resize DB if necessary
		my $current_cache_size = db_count_logged($dbh);
		if ($opt{cache_max} > 0 && $current_cache_size > $opt{cache_max}) {
			db_resize($dbh, $current_cache_size - $opt{cache_max});
		}	

		# HTML logging
		if ($opt{html_logging}) {
			if ($opt{html_logging_locked}) {
				do_locked($opt{html_logging_lockfile},
					\&html_log_urls, $info, $url_map);
			} else {
				html_log_urls($info, $url_map);
			}
		}

		# CSV logging
		if ($opt{csv_logging}) {
			if ($opt{csv_logging_locked}) {
				do_locked($opt{csv_logging_lockfile},
					\&csv_log_urls, $info, $url_map);
			} else {
				csv_log_urls($info, $url_map);
			}
		}
	};
	print_err($@) if $@;
	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub launch_browser($) {
	my($url) = @_;
	my($cmd);

	unless ($opt{browser_command_write}) {
		if (($cmd = $opt{browser_command}) =~ s/__URL__/$url/g) {
			print_err("browser command failed: $!") if system($cmd) == -1;
		} else {
			print_err("can't insert URL into browser command");
		}
	} else {
		local *CMD;
		unless (open(CMD, "| $opt{browser_command}")) {
			print_err("browser command failed: $!");
			return;
		}
		print CMD "$url\n";
		close(CMD);
	}
}

sub fix($) {
	my($t) = @_;
	$t =~ s/^\t+//gm;
	return $t;
}

# ----------- [ File locking ] --------------------------------------

sub lock_create($) {
	my($lock_filename) = @_;
	my $dir = dirname($lock_filename);

	die "directory $dir doesn't exist or isn't accessible"
		if !(-d $dir && -w _ && -r _ && -x _);

	local *LFH;
	sysopen LFH, $lock_filename, O_RDONLY | O_CREAT
		or die "Can't open or create lockfile $lock_filename: $!";
	flock LFH, LOCK_EX | LOCK_NB
		or die "Can't exclusively lock $lock_filename: $!";

	my $fh = *LOCK_FH;
	return $fh;
}

sub do_locked($$;@) {
	my $lock_filename = shift || confess;
	my $operation     = shift || confess;
	my($lock, $error);

	$lock = lock_create($lock_filename);

	eval { $operation->(@_) };
	$error = $@ if $@;

	eval { close $lock };
	die $error if $error;
}

# ----------- [ CSV logging ] ---------------------------------------

sub csv_log_urls($$) {
	my($info, $urls) = @_;
	my($template, $record, $csv);

	$template = join $opt{csv_separator}, $info->{nick},
		$info->{channel}, $info->{chatnet}, time();

	open($csv, '>>', $opt{csv_logfile})
		or die "Can't open CSV logfile $opt{csv_logfile}: $!";

	for my $url (keys %$urls) {
		$record = $template;
		$record .= $opt{csv_separator} . $url;
		print $csv "$record\n";
	}

	close $csv;
}

# ----------- [ HTML logging ] --------------------------------------

sub html_create_normal($) {
	my($filename) = @_;
	local *FH;
	open(FH, '>', $filename)
		|| die "can't create logfile $filename: $!";
	print FH <<EOF;
<?xml version="1.0" encoding="iso-8859-1"?>
	<!DOCTYPE html
		PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
		"DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
	<head>
		<title>IRC-URLs</title>
		<meta http-equiv="cache-control" content="no-cache" />
		<meta http-equiv="refresh" content="$opt{html_file_reloadtime};" />
		<style type="text/css">
		<!--
			.small { font-size: small; }
			.xsmall { font-size: x-small; }
		-->
		</style>
	</head>
	<body>
		<h1>IRC-URLs</h1>
		<p class="xsmall">
			Visit <a href="http://www.geekmind.net">geekmind.net</a>
		</p>
		<p>This page reloads itself every $opt{html_file_reloadtime} seconds.</p>
		<p>
			<a name="top" />
			<a class="small" href="#bottom">Page bottom</a>
			<br />
			<br />
		</p>
		<table rules="rows" frame="void" width="100%" cellpadding="5">
			<tr align="left">
				<th><b>Date/Time</b></th>
				<th><b>Chatnet</b></th>
				<th><b>Channel</b></th>
				<th><b>Nick</b></th>
				<th><b>URL</b></th>
			</tr>
EOF
	html_print_tail(\*FH);
	close FH;
}

sub html_create_channel {
	my $filename = shift;
	my %args     = @_;

	confess unless exists $args{CHANNEL};
	confess unless exists $args{CHATNET};
	confess unless exists $args{NORMAL_LOGFILE_REL};

	local *FH;
	open(FH, '>', $filename)
		|| die "can't create logfile $filename: $!";
	print FH <<EOF;
<?xml version="1.0" encoding="iso-8859-1"?>
	<!DOCTYPE html
		PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
		"DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
	<head>
		<title>IRC-URLs of $args{CHATNET}/#$args{CHANNEL}</title>
		<meta http-equiv="cache-control" content="no-cache" />
		<meta http-equiv="refresh" content="$opt{html_file_reloadtime};" />
		<style type="text/css">
		<!--
			.small { font-size: small; }
			.xsmall { font-size: x-small; }
		-->
		</style>
	</head>
	<body>
		<h1>IRC-URLs of $args{CHATNET}/#$args{CHANNEL}</h1>
		<p class="xsmall">
			Visit <a href="http://www.geekmind.net">geekmind.net</a>
		</p>
		<p>This page reloads itself every $opt{html_file_reloadtime} seconds.</p>
		<p><a href="$args{NORMAL_LOGFILE_REL}">Complete</a> listing.</p>
		<p>
			<a name="top" />
			<a class="small" href="#bottom">Page bottom</a>
			<br />
			<br />
		</p>
		<table rules="rows" frame="void" width="100%" cellpadding="5">
			<tr align="left">
				<th><b>Date/Time</b></th>
				<th><b>Nick</b></th>
				<th><b>URL</b></th>
			</tr>
EOF
	html_print_tail(\*FH);
	close(FH);
}

sub html_print_tail($) {
	my($fh) = @_;
	print $fh <<EOF;

@{[ LOG_FILE_MARKER ]}
		</table>
		<p>
			<a class="small" href="#top">Page top</a>
			<a name="bottom" />
		</p>
	</body>
</html>
EOF
}

sub html_print_channel {
	my $fh   = shift;
	my %args = @_;

	confess unless exists $args{DATE};
	confess unless exists $args{NICK};
	confess unless exists $args{URL};

	print $fh qq|\t\t\t<tr>\n|;
	print $fh qq|\t\t\t\t<td>$args{DATE}</td>\n|;
	if (defined($args{NICK}) && $args{NICK}) {
		print $fh qq|\t\t\t\t<td><em>$args{NICK}</em></td>\n|;
	} else {
		print $fh qq|\t\t\t\t<td></td>\n|;
	}
	print $fh qq|\t\t\t\t<td><a href="$args{URL}">$args{URL}</a></td>\n|;
	print $fh qq|\t\t\t</tr>\n|;
}

sub html_print_normal {
	my $fh   = shift;
	my %args = @_;

	confess unless exists $args{DATE};
	confess unless exists $args{NICK};
	confess unless exists $args{CHATNET};
	confess unless exists $args{CHANNEL};
	confess unless exists $args{CHANNEL_LOGFILE_REL};
	confess unless exists $args{URL};

	print $fh qq|\t\t\t<tr>\n|;
	print $fh qq|\t\t\t\t<td>$args{DATE}</td>\n|;
	print $fh qq|\t\t\t\t<td>$args{CHATNET}</td>\n|;
	if (defined($args{CHANNEL}) && $args{CHANNEL}) {
		print $fh qq|\t\t\t\t<td><a href="$args{CHANNEL_LOGFILE_REL}">#$args{CHANNEL}</a></td>\n|;
	} else {
		print $fh qq|\t\t\t\t<td></td>\n|;
	}
	if (defined($args{NICK}) && $args{NICK}) {
		print $fh qq|\t\t\t\t<td><em>$args{NICK}</em></td>\n|;
	} else {
		print $fh qq|\t\t\t\t<td></td>\n|;
	}
	print $fh qq|\t\t\t\t<td><a href="$args{URL}">$args{URL}</a></td>\n|;
	print $fh qq|\t\t\t</tr>\n|;
}

sub html_open($) {
	my($filename) = @_;
	my($fh, $pos, $buf, @lines, $off, $got_it, $hint);
	local *FH;
	$hint = "Conside manual removal of this file";
	sysopen(FH, $filename, O_RDWR) 
		|| die "can't open $filename: $!";
	$pos = sysseek(FH, 0, 2) 
		|| die "can't seek to EOF in $filename. ${hint}: $!";
	$pos -= BACKWARD_SEEK_BYTES;
	sysseek(FH, $pos, 0) 
		|| die "can't seek backwards to $pos in ${filename}; ${hint}: $!";
	sysread(FH, $buf, 2048)
		|| die "can't read rest of ${filename}; ${hint}: $!";
	$off = 0;
	@lines = split /\n/, $buf;
	for (@lines) {
		$off += length;
		$off += 1;
		chomp;
		next if /^$/;
		if (/@{[ LOG_FILE_MARKER ]}/io) {
			$got_it = 1;
			$off -= length;
			$off -= 1;
			last;
		}
	}
	die "Can't locate @{[ LOG_FILE_MARKER ]} in ${filename}; ${hint}" 
		unless $got_it;
	$pos += $off;
	sysseek(FH, $pos, 0)
		|| die "Can't seek to $pos in ${filename}; ${hint}: $!";
	# Can't pass back localized typeglob reference
	$fh = *FH;
	return $fh;
}

sub html_log_urls($$) {
	my($info, $urls) = @_;

	# replace spaces in date string to show up as '&#160' to prevent line
	# breaks in HTML code.
	my $html_date = POSIX::strftime($opt{html_date_format}, localtime($info->{timestamp}));
	$html_date =~ s/ /\&#160;/g;

	if (!(-d $opt{html_basedir} && -w _ && -r _ && -x _)) {
		die "directory $opt{html_basedir} doesn't exist or isn't accessible";
	}

	# build filename of the channel logfile and full logfile
	my $channel_logfile_rel = "$info->{chatnet}_$opt{html_channel_prefix}$info->{channel}.html";
	my $normal_logfile_rel  = $opt{html_filename};
	my $channel_logfile_abs = catfile($opt{html_basedir}, $channel_logfile_rel);
	my $normal_logfile_abs  = catfile($opt{html_basedir}, $normal_logfile_rel);

	# HTML files are only recreated (resized to 0) if the maxsize is > 0
	my $max = $opt{html_file_maxsize};

	# channel logging
	if ($opt{html_channel_logging} && $info->{channel}) {
		html_create_channel($channel_logfile_abs,
			CHANNEL            => $info->{channel},
			CHATNET            => $info->{chatnet},
			NORMAL_LOGFILE_REL => $normal_logfile_rel)
				if not -r $channel_logfile_abs or $max > 0 and -s _ > $max;

		my $fh = html_open($channel_logfile_abs);

		html_print_channel($fh,
			URL  => $_,
			DATE => $html_date,
			NICK => $info->{nick})
				for keys %$urls;

		html_print_tail($fh);
		close $fh;
	}

	# normal logging
	html_create_normal($normal_logfile_abs)
		if not -r $normal_logfile_abs or $max > 0 and -s _ > $max;

	my $fh = html_open($normal_logfile_abs);

	html_print_normal($fh,
		URL                 => $_,
		DATE                => $html_date,
		NICK                => $info->{nick},
		CHANNEL             => $info->{channel},
		CHATNET             => $info->{chatnet},
		CHANNEL_LOGFILE_REL => $channel_logfile_rel)
			for keys %$urls;

	html_print_tail($fh);
	close $fh;
}

# ----------- [ DBI database functions ] ----------------------------

sub db_dbi_connect() {
	my($dbh);
	eval { $dbh = DBI->connect($opt{db_dbi_dsn}, $opt{db_dbi_user}, $opt{db_dbi_passwd}, {
			PrintError => 0,
			RaiseError => 1,
			ShowErrorStatement => 1,
		}) or die $DBI::errstr;
	};
	confess $@ if $@;
	$dbh->{Warn} = 1;
	return $dbh;
}

sub db_dbi_disconnect($) {
	my($dbh) = @_;
	$dbh->disconnect();
}

sub db_dbi_get_urls($;$) {
	my($dbh, $limit) = @_;
	my $sql = 'SELECT url, nick, channel, chatnet, ts_added FROM url_tbl ORDER BY ts_added ';

        if (defined($limit)) {
                $sql .= "DESC LIMIT $limit";
        } else {
                $sql .= 'ASC';
        }

        my $records = $dbh->selectall_arrayref($sql);
        @$records = reverse @$records if defined($limit);
        return $records;
}

sub db_dbi_get_nth_url($$$$) {
	my($dbh, $index, $channel, $chatnet) = @_;
	$index = 1 unless $index;

	my $sql = 'SELECT url_digest FROM url_tbl ';
	if ($channel && $chatnet) {
		$sql .= "WHERE chatnet = '$chatnet' AND channel = '$channel' ";
	}
	$sql .= "ORDER BY ts_added DESC LIMIT $index";

	my $udi = $dbh->selectall_arrayref($sql);

	if (defined($udi) && @$udi) {
		$udi = $udi->[$index-1]->[0];
		$sql = 'SELECT url, nick, channel, chatnet, ts_added FROM url_tbl '
			. "WHERE url_digest = '$udi'";
		my @rec = $dbh->selectrow_array($sql);
		return $rec[0];
	}

	return undef;
}

sub db_dbi_log_urls($$$) {
	my($dbh, $info, $urls) = @_;
	my($sql, $sth);

	if ($info->{type} eq TYPE_PRIVMSG) {
		$sql = 'INSERT INTO url_tbl '
			. '(ts_added, url_digest, url, nick, chatnet) VALUES '
			. '(now(), ?, ?, ?, ?)';
	} else {
		$sql = 'INSERT INTO url_tbl '
			. '(ts_added, url_digest, url, nick, channel, chatnet) VALUES '
			. '(now(), ?, ?, ?, ?, ?)'; 
	}

	$sth = $dbh->prepare($sql);
	$dbh->begin_work();
	eval {
		if ($info->{type} eq TYPE_PRIVMSG) {
			$sth->execute($urls->{$_}, $_, $info->{nick},
				$info->{chatnet}) for keys %$urls;
		} else {
			$sth->execute($urls->{$_}, $_, $info->{nick},
				$info->{channel}, $info->{chatnet}) for keys %$urls;
		}
		$dbh->commit();
	};
	$sth->finish();
	if ($@) {
		$dbh->rollback();
		confess $@;
	}
}

sub db_dbi_filter_unknown_urls($$) {
	my($dbh, $urls) = @_;
	my($sth, %unknown_urls);

	$sth = $dbh->prepare('SELECT url_digest FROM url_tbl WHERE url_digest = ?');

	for my $url (keys %$urls) {			# iterate over raw urls
		$sth->execute($urls->{$url});		# select with url_digest
		unless ($sth->fetchrow_array()) {	# no results => unknown url
			$unknown_urls{$url} = $urls->{$url};
		}
	}
	$sth->finish();
	return \%unknown_urls;
}

sub db_dbi_count_logged($) {
	my($dbh) = @_;
	my($sth, $count);

	return scalar($dbh->selectrow_array('SELECT COUNT(ts_added) FROM url_tbl'));
}

sub db_dbi_resize($$) {
	my($dbh, $delete_count) = @_;
	my($sth, $timestamps);

	$dbh->begin_work();
	eval {
		$timestamps = $dbh->selectall_arrayref(
			"SELECT ts_added FROM url_tbl ORDER BY ts_added ASC LIMIT $delete_count");
		$sth = $dbh->prepare('DELETE FROM url_tbl WHERE ts_added = ?');
		for my $ts (@$timestamps) {
			$sth->execute($ts->[0]);
		}
	};
	$sth->finish() if $sth;
	unless ($@) {
		$dbh->commit();
	} else {
		$dbh->rollback();
		confess $@;
	}
}

sub db_dbi_stats($) {
	my($dbh) = @_;
	my($channels, %stats);

	eval {
		$channels = $dbh->selectall_arrayref('SELECT DISTINCT channel FROM url_tbl');
		for my $channel (@$channels) {
			next unless $channel->[0];
			$stats{$channel->[0]} = scalar($dbh->selectrow_array(
				'SELECT COUNT(ts_added) FROM url_tbl WHERE channel = '
				. $dbh->quote($channel->[0])));
		}
	};

	return \%stats;
}

# ----------- [ File database functions ] ---------------------------

sub db_file_connect() {
	my $h_fname = catfile($opt{db_file_basedir}, $opt{db_file_hash_filename});
	my $a_fname = catfile($opt{db_file_basedir}, $opt{db_file_array_filename});
	my(%db_h, @db_a, $lock);

	if ($opt{db_file_locking}) {
		$lock = lock_create($opt{db_file_lockfile});
	}

	eval {
		tie(%db_h, 'DB_File', $h_fname, O_RDWR | O_CREAT, 0666)
			or confess "Can't tie url db(H) to $h_fname: $!";

		tie(@db_a, 'DB_File', $a_fname, O_RDWR | O_CREAT, 0666, $DB_RECNO)
			or confess "Can't tie url db(A) to $a_fname: $!";
	};

	if ($@) {
		my $error = $@;
		eval {close $lock };
		die $error;
	}

	if (defined($lock)) {
		return { db_h => \%db_h, db_a => \@db_a, lockfh => $lock };
	}

	return { db_h => \%db_h, db_a => \@db_a };
}

sub db_file_disconnect($) {
	my($dbh) = @_;
	my($db_h, $db_a, $lockfh) = db_file_get_dbs($dbh);
	my($err_h, $err_a, $err_l);

	eval { untie %$db_h };
	$err_h = $@ if $@;

	eval { untie @$db_a };
	$err_a = $@ if $@;

	if (defined($lockfh)) {
		eval { close $lockfh };
		$err_l = $@ if $@;
	}

	confess $err_h if defined $err_h;
	confess $err_a if defined $err_a;
	confess $err_l if defined $err_l;
}

sub db_file_get_urls($;$) {
	my($dbh, $limit) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);

	my($count, @records) = (1);
	for my $url_digest (reverse @$db_a) {
		my $r = db_file_thaw_record($db_h->{$url_digest});

		push @records, [
                        $r->{url},
                        $r->{nick},
                        $r->{channel},
			$r->{chatnet},
			$r->{ts_added},
                ];

                ++$count;
                last if defined($limit) and $count > $limit;
        }

        @records = reverse @records;
        return \@records;
}

sub db_file_get_nth_url($$$$) {
	my($dbh, $index, $channel, $chatnet) = @_;
	my($records);

	$index = 1 unless defined($index); # limit to last URL
	$records = db_file_get_urls($dbh, $index);

	if (defined($records) && @$records) {
		$records = $records->[0];
		$records = $records if defined($channel) && $channel && $records->[2] eq $channel;
		$records = $records if defined($chatnet) && $chatnet && $records->[3] eq $chatnet;
		return $records->[0] if @$records; # URL
	}
	return undef;
}

sub db_file_freeze_record($$$$$) {
	my($url, $ts_added, $nick, $channel, $chatnet) = @_;

	my $record = { 
		url		=> $url,
		ts_added	=> $ts_added,
		nick		=> $nick,
		channel		=> $channel,
		chatnet		=> $chatnet,
	};

	local $Data::Dumper::Indent = 0;    # turn off all pretty print
	return Data::Dumper->Dump([$record], [qw(rec)]);
}

sub db_file_thaw_record($) {
	my($record_string) = @_;

	$deparse_sandbox = Safe->new() unless defined($deparse_sandbox);

	# $record_string is like $rec = { .... };
	$deparse_sandbox->reval(qq{
		sub reconstruct_record {
			my $record_string;
			return \$rec;
		}
	}, 1);
	confess "Can't compile deparse function: $@" if $@;

	my $record = $deparse_sandbox->reval('reconstruct_record()');
	confess "Can't deparse record: $@" if $@;

	return $record;
}

sub db_file_get_dbs($) {
	my($dbh) = @_;

	if (exists $dbh->{lockfh}) {
		return ($dbh->{db_h}, $dbh->{db_a}, $dbh->{lockfh} );
	}

	return ($dbh->{db_h}, $dbh->{db_a});
}

sub db_file_log_urls($$$) {
	my($dbh, $info, $urls) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);
	my $ts_added = time();

	for my $url (keys %$urls) {
		my $url_digest = $urls->{$url};
		# store record with url_digest as key
		$db_h->{$url_digest} = db_file_freeze_record(
			$url,
			$info->{timestamp},
			$info->{nick},
			$info->{channel},
			$info->{chatnet},
		);
		# push url_digest to end of file: seems to work better on some
		# systems
		push @$db_a, $url_digest;
	}
}

sub db_file_filter_unknown_urls($$) {
	my($dbh, $urls) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);

	my %unknown_urls;
	for my $url (keys %$urls) {
		unless (exists $db_h->{$urls->{$url}}) {
			$unknown_urls{$url} = $urls->{$url};
		}
	}

	return \%unknown_urls;
}

sub db_file_count_logged($) {
	my($dbh) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);

	return scalar(@$db_a);
}

sub db_file_resize($$) {
	my($dbh, $delete_count) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);

	for my $url_digest (splice(@$db_a, 0, $delete_count)) {
		delete $db_h->{$url_digest};
	}
}

sub db_file_stats($) {
	my($dbh) = @_;
	my($db_h, $db_a) = db_file_get_dbs($dbh);
	my(%stats);

	for my $url_digest (@$db_a) {
		my $r = db_file_thaw_record($db_h->{$url_digest});
		next unless $r->{channel};
		++$stats{$r->{channel}};
	}

	return \%stats;
}

# ----------- [ Database abstraction ] ------------------------------

sub db_connect() {
	return $db_abstraction{$opt{db_mode}}->{db_connect}->(@_);
}

sub db_disconnect($) {
	return $db_abstraction{$opt{db_mode}}->{db_disconnect}->(@_);
}

sub db_get_urls($;$) {
	return $db_abstraction{$opt{db_mode}}->{db_get_urls}->(@_);
}

sub db_get_nth_url($$$$) {
	return $db_abstraction{$opt{db_mode}}->{db_get_nth_url}->(@_);
}

sub db_log_urls($$$) {
	return $db_abstraction{$opt{db_mode}}->{db_log_urls}->(@_);
}

sub db_filter_unknown_urls($$) {
	return $db_abstraction{$opt{db_mode}}->{db_filter_unknown_urls}->(@_);
}

sub db_count_logged($) {
	return $db_abstraction{$opt{db_mode}}->{db_count_logged}->(@_);
}

sub db_resize($$) {
	return $db_abstraction{$opt{db_mode}}->{db_resize}->(@_);
}

sub db_stats($) {
	return $db_abstraction{$opt{db_mode}}->{db_stats}->(@_);
}

# ----------- [ IRSSI signals, commands, ] --------------------------

sub signal_public_message {
	my($server, $rawtext, $nick, $hostmask, $channel) = @_;

	# ignore messages to myself
	return if defined($nick) and $nick eq $server->{nick};

	# the '#' hash mark is never stored, it will be added to output if
	# needed
	$channel =~ s/^\#//;

	# check if logging is permited or denied by nick and/or channel
	return unless logging_permited($nick, $channel);

	url_log({ type      => TYPE_PUBMSG,
		  nick      => $nick, 
		  channel   => lc $channel,
		  chatnet   => $server->{chatnet},
	  	  timestamp => time() }, 
		$rawtext);
}

sub signal_private_message {
	my($server, $rawtext, $nick, $hostmask) = @_;

	# ignore messages to myself
	return if defined($nick) and $nick eq $server->{nick};

	# check if logging is permited or denied by nick
	return unless logging_permited($nick, undef);

	url_log({ type      => TYPE_PRIVMSG,
		  nick      => $nick,
		  chatnet   => $server->{chatnet},
	  	  timestamp => time() },
		$rawtext);
}

sub signal_topic_message {
	my($server, $channel, $topic, $nick, $hostmask) = @_;

	# sanitice nick
	$nick = $1 if $nick =~ /^([^!]+)/;

	# ignore messages to myself
	return if $nick eq $server->{nick};

	# the '#' hash mark is never stored, it will be added to output if
	# needed
	$channel =~ s/^\#//;

	# check if logging is permited or denied by channel
	return unless logging_permited($nick, $channel);

	url_log({ type      => TYPE_TOPIC,
		  nick      => $nick,
		  channel   => lc $channel,
		  chatnet   => $server->{chatnet},
	  	  timestamp => time() },
		$topic);
}

sub signal_channel_joined {
	my($channel) = @_;
	my $nick         = $channel->{topic_by} || undef;
	my $topic        = $channel->{topic}    || undef;
	my $channel_name = $channel->{name};

	# no topic no fun
	return unless defined($topic) or $topic;

	# the '#' hash mark is never stored, it will be added to output if
	# needed
	$channel_name =~ s/^\#//;

	# sanitice nick
	$nick = $1 if $nick =~ /^([^!]+)/;

	# If we set the topic it seems that 'topic_by' contains our nick plus
	# our hostmask and not just the nick
	return if "$channel->{ownnick}->{nick}!$channel->{ownnick}->{host}" eq $nick
		or $channel->{ownnick}->{nick} eq $nick;

	# check if logging is permited or denied by channel and/or nick
	return unless logging_permited($nick, $channel);

	url_log({ type      => TYPE_TOPIC,
		  nick      => $nick,
		  channel   => lc $channel_name,
		  chatnet   => $channel->{server}->{chatnet},
		  timestamp => time() },
		$topic);
}

sub signal_set {
	my($setting, $value) = split /\s+/, $_[0];

	if ($setting =~ /^url_/ and defined($value) and $value) {
		load_settings();
	}
}

sub signal_complete {
	my($list, $window, $word, $linestart, $want_space) = @_;

	# some people use something other than '/' to start commands
	return unless $linestart =~ m|^.url ?|i;

	$linestart =~ s|/url ?||i;

	if ($linestart =~ /^$/) {					# begin of line
		if ($word =~ s/^-//) {					# only a dash
			if ($word =~ /^$/) {				# empty
				push @$list, "-$_" for @URL_COMMANDS;
			} else {
				for my $cmd (@URL_COMMANDS) {		# known command?
					if ($cmd =~ /^$word/i) {
						push @$list, "-$cmd";
						last;
					}
				}
			}
		}
	} elsif ($linestart =~ /^-db-create$/) {
		@$list = keys %KNOWN_DBMS;
	} elsif ($linestart =~ /^-db-create\s+(?:@{[ join '|', keys %KNOWN_DBMS ]})$/o) {
		@$list = qw(-force);
	} elsif ($linestart =~ /^-help-setting$/) {
		if ($word =~ /^$/) {
			@$list = @URL_SETTINGS;
		} else {
			for my $setting (@URL_SETTINGS) {
				if ($setting =~ /^$word/i) {
					push @$list, $setting;
					last;
				}
			}
		}
	}

	Irssi::signal_stop();
}

sub command_url_list(;$) {
	my($limit) = @_;
	my($dbh);

	eval {
		$dbh   = db_connect();
		$limit = $opt{list_default_limit} unless defined $limit;
		$limit = undef if $limit == -1;
		my $rows = db_get_urls($dbh, $limit);
		unless (@$rows) {
			db_disconnect($dbh);
			print_out('urlplot: no entries available');
			return;
		}

		command_url_list_helper($rows);
	};

	print_err($@) if $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_list_helper($) {
	my($rows) = @_;

	# Calculate maximum with of items
	my($width_chatnets, $width_nicks, $width_channels);
	for my $r (@$rows) {         # (url, nick, channel, chatnet)
		$r->[0] =~ s/%/%%/g; # irssi uses '%' as fmt
		$r->[1] ||= '';
		$r->[2] = $r->[2] ? "#$r->[2]" : '';
		$r->[3] ||= '';
		# chatnets
		my $len = length $r->[3];
		$width_chatnets = $len if $len > $width_chatnets;
		# nicks
		$len = length $r->[1];
		$width_nicks = $len if $len > $width_nicks;
		# channels
		$len = length $r->[2];
		$width_channels = $len if $len > $width_channels;
	}

	$width_chatnets = 10 if $width_chatnets < 10;
	$width_nicks    = 15 if $width_nicks    < 15;
	$width_channels = 15 if $width_channels < 15;

	my $mk_label = sub {
		use integer;
		my($label, $width) = @_;
		my($len, $rest);
		# we assume label is always shorter than the minimal
		# length of '$width_*'
		$rest  = ($$width + 1 - length($label)) / 2;
		$rest  = ' ' x $rest;
		$label = "${rest}${label}${rest}";
		# correct with if the new label is taller than the
		# original width
		$len   = length $label;
		$$width = $len if $len  > $$width;
		return $label;
	};

	my $banner = $mk_label->(' Chatnet ', \$width_chatnets) . '|' 
		. $mk_label->(' Channel ', \$width_channels) . ' |'
		. $mk_label->(' Nick ', \$width_nicks) 
		. ' | URL';

	# make horizontal line at least 80 characters wide
	my $blen = length $banner;
	$blen = 80 if $blen < 80;

	print_out('-' x $blen);
	print_out($banner);
	print_out('-' x $blen);

	for my $r (@$rows) {
		print_out(sprintf("%-${width_chatnets}s| %-${width_channels}s| %-${width_nicks}s| %s",
			$r->[3], $r->[2], $r->[1], $r->[0]));
	}
}

sub command_url_sync_topics() {
	my @channels = Irssi::channels();
	if (@channels) {
		signal_channel_joined($_) for @channels;
		print_out("urlplot: " . scalar(@channels) . " channel topics scanned");
	}
}

sub command_url_html_recreate() {
	my($dbh, $count);

	eval {
		$dbh = db_connect();
		my $rows = db_get_urls($dbh);
		unless (@$rows) {
			db_disconnect($dbh);
			print_out('urlplot: no entries available');
			return;
		}

		$count = @$rows;

		my($info, $url_map);
		for my $r (@$rows) { # (url, nick, channel, chatnet)
			$info = {    # type is ignored with html-logging
				nick      => $r->[1],
				channel   => $r->[2],
				chatnet   => $r->[3],
				timestamp => $r->[4],
			};

			# we fake here because html_log_urls doesn't needs the
			# url_digest of the urls we supply here; thus create a
			# hash with: url => 'fake'
			$url_map = { $r->[0] => __FILE__ . __LINE__ };

			if ($opt{html_logging_locked}) {
				do_locked($opt{html_logging_lockfile},
					\&html_log_urls, $info, $url_map);
			} else {
				html_log_urls($info, $url_map);
			}
		}
	};

	print_err($@) if $@;
	print_out("urlplot: HTML files recreated ($count URLs)") unless $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_stats() {
	my($dbh, $channels);

	eval {
		use integer;

		$dbh = db_connect();
		$channels = db_stats($dbh);

		my(%count_2_name, $count_total, $width_channels);
		for my $channel (keys %$channels) {
			$count_2_name{$channels->{$channel}} = $channel;
			$count_total += $channels->{$channel};
			# calculate max width of channels
			my $len = length $channel;
			$width_channels = $len if $len > $width_channels;
		}

		$width_channels = 15 if $width_channels < 15;
		++$width_channels if ($width_channels % 2) == 0;

		my $banner = 'Channel';
		my $left_right = ($width_channels - length($banner)) / 2;
		$banner = (' ' x $left_right) . $banner . (' ' x $left_right);

		my $right_banner = '|  Count  ';
		my $blen = length($banner) + length($right_banner);
		
		print_out('-' x $blen);
		print_out($banner . $right_banner);
		print_out('-' x $blen);

		for my $count (sort { $a <=> $b } keys(%count_2_name)) {
			print_out(sprintf("%-${width_channels}s| %-5d", $count_2_name{$count}, $count));
		}

		print_out('-' x $blen);
		print_out("Total: $count_total");
	};

	print_err($@) if $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_db_select_where(@) {
	my(@args) = @_;
	my($dbh, $sql);

	unless ($opt{db_mode} eq 'DBI') {
		print_err("-db-select-where doesn't apply to file-based DBMS");
		return;
	}
	unless (@args) {
		print_err('missing arguments');
		return;
	}

	# Keep this in sync with db_dbi_get_urls
	$sql = "SELECT url, nick, channel, chatnet, ts_added FROM url_tbl WHERE @args";

	eval {
		$dbh = db_connect();
		my $records = $dbh->selectall_arrayref($sql);
		unless (@$records) {
			print_out('urlplot: empty result set');
			db_disconnect($dbh);
			return;
		}
		# see db_dbi_get_urls for this
		@$records = reverse @$records if $sql =~ /(?:DESC|LIMIT)/ and @$records;
		command_url_list_helper($records);
	};

	print_err($@) if $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_db_create($$) {
	my($dbms, $force) = @_;

	unless ($opt{db_mode} eq 'DBI') {
		print_err("-db-create doesn't apply to file-based DBMS");
		return;
	}

	unless (exists $KNOWN_DBMS{$dbms}) {
		print_err("supported DBMS are: '@{[ keys %KNOWN_DBMS ]}'");
		return;
	}

	unless ($force) {
		print_out(<<EOF);
Use a sequence of commands _like_ the following
to prepare a database for use with urlplot:
EOF

	if ($dbms eq 'postgresql') {
		print_out(<<EOF);
Install DBD::Pg
shell> su -l pgsql
shell> createuser <YOUR-DB-USER>
shell> createdb url_db
shell> psql url_db
psql> ALTER USER <YOUR-DB-USER> WITH PASSWORD 'some_pass';
EOF
	} elsif ($dbms eq 'mysql') {
		print_out(<<EOF);
Install DBD::mysql
shell> mysql -u root -p
mysql> use mysql;
mysql> CREATE DATABASE url_db;
mysql> GRANT ALL PRIVILEGES ON url_db.* TO 
  <YOUR-DB-USER>\@localhost IDENTIFIED BY 'some_pass';
EOF
	}

	print_out(<<EOF);
and in IRSSI:
/set url_db_dbi_user <YOUR-DB-USER>
/set url_db_dbi_passwd some_pass
EOF

	if ($dbms eq 'postgresql') {
		print_out('/set url_db_dbi_dsn dbi:Pg:dbname=url_db;host=<DB-HOST>');
	} elsif ($dbms eq 'mysql') {
		print_out('/set url_db_dbi_dsn DBI:mysql:database=url_db;host=<DB-HOST>');
	}

	print_out(<<EOF);
Of course access-control is out of the scope of this short intro.

Create the database table with the following command:
/url -db-create -force $dbms
EOF
		return;
	}

	my($sql, $dbh);
	$sql = 'CREATE TABLE url_tbl ('
			. 'ts_added        TIMESTAMP    NOT NULL,'
			. 'url_digest      CHAR(32)     PRIMARY KEY,'
			. 'url             TEXT         NOT NULL,'
			. 'nick            VARCHAR(128) NULL,'
			. 'channel         VARCHAR(128) NULL,'
			. 'chatnet         VARCHAR(64)  NOT NULL)';

	eval {
		$dbh = db_connect();
		my $sth = $dbh->prepare($sql);
		$sth->execute();
		$sth->finish();
	};

	print_err($@) if $@;
	print_out('urlplot: database schema created') unless $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_db_flush() {
	my $dbh;

	eval {
		$dbh = db_connect();
		my $size = db_count_logged($dbh);
		db_resize($dbh, $size);
	};

	print_err($@) if $@;
	print_out('urlplot: database flushed') unless $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;
}

sub command_url_help() {
	my($text);
	$text = fix(<<"	EOF");
	%9SYSNOPSIS%9

	%9/url%9 %U[OPTIONS|INTEGER]

	Browses to the last logged URL depending on the active window or
	to the last logged URL globally if not in an channel window.

	An optional integer may select the last nth URL logged.

	%9OPTIONS%9

	%9-list%9 %U[LIMIT-COUNT]
	    List logged URLs.
	    Limits the listing to LIMIT-COUNT URLs if given.

	%9-browse-index%9
	    Browse the main HTML logfile.

	%9-sync-topics%9
	    Scans all channel topics and logs the URLs found.

	%9-html-recreate%9
	    Recreates all HTML files from the database.
	    This also works if the setting %9url_html_logging%9 is disabled.

	%9-stats%9
	    Displays statistics of URLs logged.

	%9-db-select-where%9 %USQL-QUERY-STRING
	    Performs a DBI RDBMS SQL query and prints results like %9-list%9.
	    Query string should be the WHERE part of a SQL-select statement.

	    Supported database columns are:
	        %Uts_added%U     : Timestamp (when logged)
	        %Uurl%U          : URL
	        %Uurl_digest%U   : MD5 digest of URL
	        %Unick%U         : Nick (maybe NULL)
	        %Uchannel%U      : Channel (maybe NULL)
	        %Uchatnet%U      : Chatnet

	%9-db-create%9 %UDMBS-NAME [ %9-force%9 ]
	    Creates the DBI RDBMS schema.
	    Currently supported DMBS are: @{[ join ', ', keys %KNOWN_DBMS ]}

	    Omitting the %9-force%9 option prints some dokumentation on the
	    required steps that need to be done before %9-db-create%9 can
	    be used.

	%9-db-flush%9
	    Deletes all entries in the database.

	%9-help-setting%9 %USETTING_NAME
	    Display help on given setting.

	%9-help -h -?%9
	    Displays this help message.

	%9SUMMARY%9

	%9/url -list [ LIMIT-COUNT ]%9    List logged urls optinally the last LIMIT-COUNT ones
	%9/url [INTEGER]%9                Browse the nth URL (index 1..N) where 1 is the last URL logged
	%9/url -browse-index%9            Browse to main HTML logfile
	%9/url -sync-topics%9             Scan all channels topics
	%9/url -html-recreate%9           Recreate all HTML files from the database
	%9/url -db-select-where SQL%9     Query DBI RDMS with SELECT .. FROM .. WHERE 'SQL'
	%9/url -db-create DBMS [-force]%9 Create DBI RBMS schema where DBMS is any of '@{[ join ', ', keys %KNOWN_DBMS ]}'
	%9/url -db-flush%9                Delete all entries in DB
	%9/url -help-setting STRING%9     Display help on given url setting
	%9/url -? | -h | -help%9          This help message

	%9EXAMPLE%9

	%9/url -db-select-where%9 (chatnet = 'Freenode' OR chatnet = 'IRCNet') AND channel = 'perl'
		      ORDER BY chatnet DESC LIMIT 50

	%9BUGS%9

	urlplot will log URLs of the form 'www.a.b,' as is. That is, if someone writes
	'it is at www.a.b, or www.b.c' urlplot will log two URLs. The first will be
	'www.a.b,' (notice the comma) and 'www.b.c'.
	EOF
	print CLIENTCRAP $text;
}

sub command_url_help_setting($) {
	my($setting) = @_;
	my($text);

	if ($setting eq 'url_print_to_active_win') {
		$text = fix(<<'		EOF');
		%9url_print_to_active_win%9 %UON|OFF

		Activates printing of normal status messages to the active
		window instead to the status window.
		EOF
	} elsif ($setting eq 'url_list_default_limit') {
		$text = fix(<<'		EOF');
		%9url_list_default_limit%9 %UINTEGER

		Limits the listing via /url -list to the last logged URLs.
		This has the effect as always saying /url -list %UINTEGER.
		EOF
	} elsif ($setting eq 'url_cache_max') {
		$text = fix(<<'		EOF');
		%9url_cache_max%9 %UINTEGER

		Specifies the maximum amount of URLs to log (cache).
		EOF
	} elsif ($setting eq 'url_html_logging') {
		$text = fix(<<'		EOF');
		%9url_html_logging%9 %UON|OFF
		
		Globally enables/disables HTML logging.
		EOF
	} elsif ($setting eq 'url_html_date_format') {
		$text = fix(<<'		EOF');
		%9url_html_date_format%9 %USTRING

		Defines a format that will be passed to (3) strftime; used as
		the date/time in HTML logfiles.
		EOF
	} elsif ($setting eq 'url_html_basedir') {
		$text = fix(<<'		EOF');
		%9url_html_basedir%9 %UDIRECTORY

		All HTML logfiles will be created below this directory.
		EOF
	} elsif ($setting eq 'url_html_filename') {
		$text = fix(<<'		EOF');
		%9url_html_filename%9 %UFILENAME

		Specifies the filename of the main HTML logfile where
		everything will be logged to. The given filename we be treated
		relative to %9url_html_basedir%9.
		EOF
	} elsif ($setting eq 'url_html_file_maxsize') {
		$text = fix(<<'		EOF');
		%9url_html_file_maxsize%9 %UINTEGER

		Size in bytes that HTML logfiles should not exceed. 
		A file will be truncated to a size of zero if it grows larger
		than the given size. 
		EOF
	} elsif ($setting eq 'url_html_file_reloadtime') {
		$text = fix(<<'		EOF');
		%9url_html_file_reloadtime%9 %UINTEGER

		Sepfies the auto reload time in seconds of HTML logfiles.
		EOF
	} elsif ($setting eq 'url_html_logging_locked') {
		$text = fix(<<'		EOF');
		%9url_html_logging_locked%9 %UON|OFF

		Enables/disables file locking of HTML logfiles. See also
		%9url_html_logging_lockfile%9.
		EOF
	} elsif ($setting eq 'url_html_logging_lockfile') {
		$text = fix(<<'		EOF');
		%9url_html_logging_lockfile%9 %UFILENAME

		The lockfile (absolute path) used to lock HTML logfiles. See
		also %9url_html_logging_locked%9.
		EOF
	} elsif ($setting eq 'url_html_channel_logging') {
		$text = fix(<<'		EOF');
		%9url_html_channel_logging%9 %UON|OFF

		Enables the creation of seperate HTML logfiles per channel.
		EOF
	} elsif ($setting eq 'url_html_channel_prefix') {
		$text = fix(<<'		EOF');
		%9url_html_channel_prefix%9 %USTRING

		Filename prefix prepend to HTML channel logfiles.
		EOF
	} elsif ($setting eq 'url_csv_logging') {
		$text = fix(<<'		EOF');
		%9url_csv_logging%9 %UON|OFF

		Enables/disables CSV logging.
		EOF
	} elsif ($setting eq 'url_csv_separator') {
		$text = fix(<<'		EOF');
		%9url_csv_separator%9 %USTRING

		Separator used to separate the fields in CVS logfiles.
		EOF
	} elsif ($setting eq 'url_csv_logfile') {
		$text = fix(<<'		EOF');
		%9url_csv_logfile%9 %UFILENAME

		Filename (absolute path) used for CSV logging.
		EOF
	} elsif ($setting eq 'url_csv_logging_locked') {
		$text = fix(<<'		EOF');
		%9url_csv_logging_locked%9 %UON|OFF

		Enables/disables file locking of CSV logfiles. See also
		%9url_csv_logging_lockfile%9.
		EOF
	} elsif ($setting eq 'url_csv_logging_lockfile') {
		$text = fix(<<'		EOF');
		%9url_csv_logging_lockfile%9 %UFILENAME

		The lockfile (absolute path) used to lock CSV logfiles. See
		also %9url_csv_logging_locked%9.
		EOF
	} elsif ($setting eq 'url_policy_default') {
		$text = fix(<<'		EOF');
		%9url_policy_default%9 %Uallow|deny

		Specifies the default policy. Thus if it's 'allow' the value
		of %9url_policy_channels%9 and %9url_policy_nicks%9 specifies
		channels and nicks for which URLs will not be logged. This
		scheme will be reversed if it's value is 'deny'.
		EOF
	} elsif ($setting eq 'url_policy_channels') {
		$text = fix(<<'		EOF');
		%9url_policy_channels%9 %Uchannel1,channel2,...

		Defines which channels will be allowed/rejected URL logging.
		EOF
	} elsif ($setting eq 'url_policy_nicks') {
		$text = fix(<<'		EOF');
		%9url_policy_nicks%9 %Unick1,nick2,...

		Defines which nicks will be allowed/rejected URL logging.
		EOF
	} elsif ($setting eq 'url_db_mode') {
		$text = fix(<<'		EOF');
		%9url_db_mode%9 %Ufile|dbi

		Operating mode of the URL database. 'file' selects the feature
		less file based database, whereas 'dbi' selects the more
		feature full DBI based database.
		
		See also the settings starting with %9url_db_file_%9 and
		%9url_db_dbi_%9.
		EOF
	} elsif ($setting eq 'url_db_dbi_dsn') {
		$text = fix(<<'		EOF');
		%9url_db_dbi_dsn%9 %UDSN

		Specifies the DBI data source name (DSN) used to access the
		database.

		Examples:

		for Postgresql:
			dbi:Pg:dbname=url_db;host=myhost

		for Mysql:
			DBI:mysql:database=url_db;host=myhost;port=myport
		EOF
	} elsif ($setting eq 'url_db_dbi_user') {
		$text = fix(<<'		EOF');
		%9url_db_dbi_user%9 %USTRING

		Your database username.
		EOF
	} elsif ($setting eq 'url_db_dbi_passwd') {
		$text = fix(<<'		EOF');
		%9url_db_dbi_passwd%9 %USTRING

		Your database password.
		EOF
	} elsif ($setting eq 'url_db_file_basedir') {
		$text = fix(<<'		EOF');
		%9url_db_file_basedir%9 %UDIRECTORY

		Base directory for the file based database (absolute path).
		EOF
	} elsif ($setting eq 'url_db_file_hash_filename') {
		$text = fix(<<'		EOF');
		%9url_db_file_hash_filename%9 %UFILENAME

		Filename (relative) of the hash based database.
		EOF
	} elsif ($setting eq 'url_db_file_array_filename') {
		$text = fix(<<'		EOF');
		%9url_db_file_array_filename%9 %UFILENAME

		Filename (relative) of the array based database.
		EOF
	} elsif ($setting eq 'url_db_file_locked') {
		$text = fix(<<'		EOF');
		%9url_db_file_locked%9 %UON|OFF

		Enables/disables file locking of the file based database files. See also
		%9url_db_file_lockfile%9.
		EOF
	} elsif ($setting eq 'url_db_file_lockfile') {
		$text = fix(<<'		EOF');
		%9url_db_file_lockfile%9 %UFILENAME

		The lockfile (absolute path) used to lock the file based database. See
		also %9url_db_file_locked%9.
		EOF
	} elsif ($setting eq 'url_browser_command_write') {
		$text = fix(<<'		EOF');
		%9url_browser_command_write%9 %UON|OFF

		Defines the main operating mode of the browser command. If
		disabled, the URL will be globally substituted in
		%9url_browser_command%9 at every instance of the placeholder
		'__URL__'. If enabled, the URL will be written to STDIN of the
		command launched by %9url_browser_command%9.

		See also %9url_browser_command%9.
		EOF
	} elsif ($setting eq 'url_browser_command') {
		$text = fix(<<'		EOF');
		%9url_browser_command%9 %USTRING

		Command to lauch on %9/url%9, %9/url%9 %Uinteger%U or %9/url -browse-index%9.

		The given string has to contain the placeholder '%9__URL__%9'
		if %9url_browser_command_write%9 is disabled.

		See also %9url_browser_command_write%9.
		EOF
	} elsif ($setting eq 'url_browse_index_url') {
		$text = fix(<<'		EOF');
		%9url_browse_index_url%9 %USTRING

		Defines the URL to browse when the command %9/url -browse-index%9 
		is called. Usefull to browse your %9url_html_filename%9.
		EOF
	} else {
		$text = fix(<<"		EOF");
		No help available for '$setting'. Please ensure that you did not
		mispell '$setting'.

		You may contact '$IRSSI{authors}' at '$IRSSI{contact}' if you
		found a setting for which no help is available to allude this
		fact.
		EOF
	}

	print CLIENTCRAP $text;
}

sub command_url_browse_index() {
	eval { launch_browser($opt{browse_index_url}) };
	print_err($@) if $@;
}

sub command_url_browse(;$$$) {
	my($nth_url, $of_channel, $of_chatnet) = @_;
	my($dbh, $url, $print_url);

	$of_channel =~ s/^\#// if $of_channel;

	eval {
		$dbh = db_connect();
		$url = db_get_nth_url($dbh, $nth_url, $of_channel, $of_chatnet);
	};

	print_err($@) if $@;

	eval { db_disconnect($dbh) } if defined($dbh);
	print_err($@) if $@;

	if (defined($url)) {
		($print_url = $url) =~ s/%/%%/g;
		print_out("browsing to $print_url");
		eval { launch_browser($url) };
		print_err($@) if $@;
	} else {
		print_err('no URL to browse; ensure you have one with /url -list');
	}
}

sub command_url {
	my($data, $server, $witem) = @_;
	$_ = $data;

	if (/^-list(\s+(\d+))?\s*$/) {
		command_url_list($2 || undef);

	} elsif (/^-sync-topics\s*$/) {
		command_url_sync_topics();

	} elsif (/^-html-recreate\s*$/) {
		command_url_html_recreate();

	} elsif (/^-stats\s*$/) {
		command_url_stats();

	} elsif (/^-db-select-where\s+(.*)\s*$/) {
		command_url_db_select_where($1);

	} elsif (/^-db-create\s+(\w+)(?:\s+(-force))?\s*$/) {
		command_url_db_create(lc $1, defined($2));

	} elsif (/^-db-flush\s*$/) {
		command_url_db_flush();

	} elsif (/^-(?:h(?:elp)?|\?)\s*$/) {
		command_url_help();

	} elsif (/^-help-setting\s+([^ ]+)\s*$/) {
		command_url_help_setting($1);

	} elsif (/^-browse-index\s*$/) {
		command_url_browse_index();

	} elsif (/^(\d+)?\s*$/) {
		my($channel, $chatnet);

		if (defined($witem) && ref($witem) && $witem->isa('Irssi::Irc::Channel')) {
			$channel = $witem->{name};
			$chatnet = $witem->{server}->{chatnet};
		}
		command_url_browse($1, $channel, $chatnet);

	} else {
		command_url_help();
	}
}

# TODO
#
# 	html: create channel index page
#
# 	urlplot registriert keine urls in ACTIONs, also bei /me	http://heise.de
#
# 	add statistics to bottom of main irc logfile and a note that reflects,
# 	that the statistics are from the database (db-stats)
#
# 	maybe add a feature to detect urls embedded in words like {www.fo}
#
# 	allow fuzzy searching?
#
# 	new option that allows to execute an command after a html file was
# 	created (eg. chmod 644 __FILE__) ??
#
# 	check if insert into can work with undef as NULL in insert; this would
# 	allow to work with one insert into statement instead of two

# vim: set ts=8 sw=8
