#!/usr/bin/perl
# This tool is "fat-packed": most of its dependent modules are embedded
# in this file.  

package remove_old_master;
use Time::HiRes qw(gettimeofday);
use strict;
use DBI;
use Getopt::Long;
use Pod::Usage;
use Data::Dumper;

$Getopt::Long::ignorecase = 0;
my $Param = {};
my $user = "admin";
my $pass = "admin";
my $help = '';
my $host = 'localhot' ;
my $debug = 0 ;
my %hostgroups;

my %processState;
my %processCommand;
my @HGIds;




######################################################################
#Local functions
######################################################################

sub URLDecode {
    my $theURL = $_[0];
    $theURL =~ tr/+/ /;
    $theURL =~ s/%([a-fA-F0-9]{2,2})/chr(hex($1))/eg;
    $theURL =~ s/<!--(.|\n)*-->//g;
    return $theURL;
}
sub URLEncode {
    my $theURL = $_[0];
   $theURL =~ s/([\W])/"%" . uc(sprintf("%2.2x",ord($1)))/eg;
   return $theURL;
}

# return a proxy object
sub get_proxy($$$$){
    my $dns = shift;
    my $user = shift;
    my $pass = shift;
    my $debug = shift;
    my $proxynode = ProxySqlNode->new();
    $proxynode->dns($dns);
    $proxynode->user($user);
    $proxynode->password($pass);
    $proxynode->debug($debug);
    
    return $proxynode;
    
}

 sub main{
    # ============================================================================
    #+++++ INITIALIZATION
    # ============================================================================
    
    if($#ARGV < 3){
	#given a ProxySQL scheduler
	#limitation we will pass the whole set of params as one
	# and will split after
	@ARGV = split('\ ',$ARGV[0]);
    }
    $Param->{user}       = '';
    $Param->{log}       = undef ;
    $Param->{password}   = '';
    $Param->{host}       = '';
    $Param->{port}       = '127.0.0.1';
    $Param->{debug}      = 0; 
    $Param->{processlist} = 0;
    $Param->{OS} = $^O;
    $Param->{main_segment} = 0;
    $Param->{retry_up} = 0;
    $Param->{retry_down} = 0;
    $Param->{print_execution} = 0;
    $Param->{development} = 0;
    $Param->{hgid} = 0;


    
    my $run_pid_dir = "/tmp" ;
    
    #if (
	GetOptions(
	    'user|u:s'       => \$Param->{user},
	    'password|p:s'   => \$Param->{password},
	    'host|h:s'       => \$host,
	    'port|P:i'       => \$Param->{port},
	    'debug|d:i'      => \$Param->{debug},
	    'log:s'      => \$Param->{log},
	    'main_segment|S:s'=> \$Param->{main_segment},
	    'retry_up:i' =>	\$Param->{retry_up},
	    'retry_down:i' =>	\$Param->{retry_down},
	    'execution_time:i' => \$Param->{print_execution},
	    'development:i' => \$Param->{development},
	    'active_failover' => \$Param->{active_failover},
        'hgid|G:i' => \$Param->{hgid},

	    
	    'help|?'       => \$Param->{help}
    
	) or pod2usage(2);
	pod2usage(-verbose => 2) if $Param->{help};

    die print Utils->print_log(1,"Option --hgid not specified.\n") unless defined($Param->{hgid});
    die print Utils->print_log(1,"Option --host not specified.\n") unless defined $Param->{host};
    die print Utils->print_log(1,"Option --user not specified.\n") unless defined $Param->{user};
    die print Utils->print_log(1,"Option --port not specified.\n") unless defined $Param->{port};
    die "Option --log not specified. We need a place to log what is going on, don't we?\n" unless defined $Param->{log};
    print Utils->print_log(2,"Option --log not specified. We need a place to log what is going on, don't we?\n") unless defined $Param->{log};
    
    if($Param->{debug}){
	Utils::debugEnv();
    }
    
    $Param->{host} = URLDecode($host);    
    my $dsn  = "DBI:mysql:host=$Param->{host};port=$Param->{port}";
    if(defined $Param->{user}){
	    $user = "$Param->{user}";
    }
    if(defined $Param->{password}){
	    $pass = "$Param->{password}";
    }
    my $hg =$Param->{hgid};
    $hg =~ s/[\:,\,]/_/g;
    my $base_path = "${run_pid_dir}/proxysql_galera_check_${hg}.pid";
    
    #============================================================================
    # Execution
    #============================================================================
    if(defined $Param->{log}){
	open(FH, '>>', $Param->{log}."_".$hg.".log") or die Utils->print_log(1,"cannot open file");
	select FH;
    }
    
    if($Param->{development} < 1){
	if(!-e $base_path){
	    `echo "$$ : $hg" > $base_path`
	}
	else{
	    print Utils->print_log(1,"Another process is running using the same HostGroup and settings,\n Or orphan pid file. check in $base_path");
	    exit 1;
	}    
    }
     
    # for test only purpose comment for prod

    my $xx =1;
    my $y =0;
    $xx=20000000 if($Param->{development} > 0);
 	
     while($y < $xx){
	++$y ;
	
    my $start = gettimeofday();    
    if($Param->{debug} >= 1){
	print Utils->print_log(3,"START EXECUTION\n");
    }
    

    
    my $proxy_sql_node = get_proxy($dsn, $user, $pass ,$Param->{debug}) ;

    $proxy_sql_node->retry_down($Param->{retry_down});
    $proxy_sql_node->move_node($proxy_sql_node,$Param->{hgid});

    $proxy_sql_node->connect();
    
    
    my $end = gettimeofday();
    print Utils->print_log(3,"END EXECUTION Total Time:".($end - $start) * 1000 ."\n\n") if $Param->{print_execution} >0; 


    
    $proxy_sql_node->disconnect();
	
    #debug braket 	
     sleep 2 if($Param->{development} > 0);
    }
    if(defined $Param->{log}){
    close FH;  # in the end
    }
    
    `rm -f $base_path`;
    
    exit(0);
    
    
 }

# ############################################################################
# Run the program.
# ############################################################################
    exit main(@ARGV);


{
    package ProxySqlNode;
    sub new {
        my $class = shift;

        my $SQL_get_monitor = "select variable_name name,variable_value value from global_variables where variable_name in( 'mysql-monitor_username','mysql-monitor_password','mysql-monitor_read_only_timeout' ) order by 1";
        my $SQL_get_hostgroups = "select distinct hostgroup_id hg_isd from mysql_servers order by 1;";
        my $SQL_get_rep_hg = "select writer_hostgroup,reader_hostgroup from mysql_replication_hostgroups order by 1;";

        # Variable section for  looping values
        #Generalize object for now I have conceptualize as:
        # Proxy (generic container)
        # Proxy->{DNS} conenction reference
        # Proxy->{PID} processes pid (angel and real)
        # Proxy->{hostgroups}
        # Proxy->{user} This is the user name
        # Proxy->{password} 
        # Proxy->{port}     node status (OPEN 0,Primary 1,Joiner 2,Joined 3,Synced 4,Donor 5)
        
        my $self = {
            _dns  => undef,
            _pid  => undef,
            _user => undef,
            _password => undef,
            _port => undef,
            _hgid => undef,
            _monitor_user => undef,
            _monitor_password => undef,
            _SQL_get_monitor => $SQL_get_monitor,
            _SQL_get_hg=> $SQL_get_hostgroups,
            _SQL_get_replication_hg=> $SQL_get_rep_hg,
            _dbh_proxy => undef,
            _check_timeout => 100, #timeout in ms
    	    _retry_down => 0, # number of retry on a node before declaring it as failed.

        };
        bless $self, $class;
        return $self;
        
    }

    sub retry_down{
        my ( $self, $in ) = @_;
        $self->{_retry_down} = $in if defined($in);
        return $self->{_retry_down};
    }

    
    sub debug{
        my ( $self, $debug ) = @_;
        $self->{_debug} = $debug if defined($debug);
        return $self->{_debug};
    }
    
    sub dns {
        my ( $self, $dns ) = @_;
        $self->{_dns} = $dns if defined($dns);
        return $self->{_dns};
    }

    sub dbh_proxy{
        my ( $self, $dbh_proxy ) = @_;
        $self->{_dbh_proxy} = $dbh_proxy if defined($dbh_proxy);
        return $self->{_dbh_proxy};
    }

    sub pid {
        my ( $self, $pid ) = @_;
        $self->{_pid} = $pid if defined($pid);
        return $self->{_pid};
    }

    
    sub user{
        my ( $self, $user ) = @_;
        $self->{_user} = $user if defined($user);
        return $self->{_user};
    }

    sub password {
        my ( $self, $password ) = @_;
        $self->{_password} = $password if defined($password);
        return $self->{_password};
    }
    
    sub monitor_user{
        my ( $self, $monitor_user ) = @_;
        $self->{_monitor_user} = $monitor_user if defined($monitor_user);
        return $self->{_monitor_user};
    }

    sub monitor_password {
        my ( $self, $monitor_password ) = @_;
        $self->{_monitor_password} = $monitor_password if defined($monitor_password);
        return $self->{_monitor_password};
    }

    sub port {
        my ( $self, $port ) = @_;
        $self->{_port} = $port if defined($port);
        return $self->{_port};
    }

    sub hgid {
        my ( $self, $hgid ) = @_;
        $self->{_hgid} = $hgid if defined($hgid);
        print Dumper($hgid);
        return $self->{_hgid};
    }

    sub check_timeout{
        my ( $self, $check_timeout ) = @_;
        $self->{_check_timeout} = $check_timeout if defined($check_timeout);
        return $self->{_check_timeout};
    }
    
    sub move_node{
     my ( $self,$proxynode,$hgid ) = @_;

        my $dbh = Utils::get_connection($self->{_dns}, $self->{_user}, $self->{_password},' ');
        $self->{_dbh_proxy} = $dbh;

        my $SQL_get_old_master= "SELECT ro.hostname,
                                       ro.port,
                                       ms.hostgroup_id
                                FROM monitor.mysql_server_read_only_log ro
                                LEFT JOIN monitor.mysql_server_replication_lag_log lag ON ro.hostname=lag.hostname
                                AND ro.port=lag.port
                                LEFT JOIN mysql_servers ms ON ro.hostname=ms.hostname
                                AND ro.port=ms.port,
                                    mysql_replication_hostgroups hg
                                WHERE read_only=1
                                    AND repl_lag IS NULL
                                    AND ms.hostgroup_id='$hgid'
                                    AND ms.hostgroup_id = hg.reader_hostgroup
                                    AND ms.max_replication_lag = 0
                                GROUP BY ro.hostname,
                                         ro.port;";

        my $sth = $dbh->prepare($SQL_get_old_master);
        my $ref;
        $sth->execute();
         while ($ref = $sth->fetchrow_arrayref()) {
            print "@$ref[0] @$ref[1]\n";
            if (@$ref[0] ne '') {
                move_node_down_hg_change($proxynode,@$ref[0],@$ref[1],$Param->{hgid});
            }

         }
        }


    #Connect method connect an populate the cluster returns the Galera cluster
    sub connect{
        my ( $self, $port ) = @_;
        my $dbh = Utils::get_connection($self->{_dns}, $self->{_user}, $self->{_password},' ');
        $self->{_dbh_proxy} = $dbh;
        
        # get monitor user/pw                
        my $cmd = $self->{_SQL_get_monitor};


        my $sth = $dbh->prepare($cmd);
        $sth->execute();
        while (my $ref = $sth->fetchrow_hashref()) {
            if($ref->{'name'} eq 'mysql-monitor_password' ){$self->{_monitor_password} = $ref->{'value'};}
            if($ref->{'name'} eq 'mysql-monitor_username' ) {$self->{_monitor_user} = $ref->{'value'};}
            if($ref->{'name'} eq 'mysql-monitor_read_only_timeout' ) {$self->{_check_timeout} = $ref->{'value'};}
            
        }
	if($self->debug >=1){print Utils->print_log(3," Connecting to ProxySQL " . $self->{_dns}. "\n" ); }
        
    }
    sub disconnect{
        my ( $self, $port ) = @_;
        $self->{_dbh_proxy}->disconnect;
	
        
    }

    #move a node to a maintenance HG ((9000 + HG id))
    sub move_node_down_hg_change{
	my ($proxynode, $host,$port,$hgid) = @_;
	
	if($hgid > 9000) {return 1;}
	
	my $node_sql_command = "SET GLOBAL READ_ONLY=1;";
	my $proxy_sql_command =" UPDATE mysql_servers SET hostgroup_id=".(9000 + $hgid)." WHERE hostgroup_id=$hgid AND hostname='$host' AND port='$port'";
    print "$proxy_sql_command\n";
	$proxynode->{_dbh_proxy}->do($proxy_sql_command) or die "Couldn't execute statement: " .  $proxynode->{_dbh_proxy}->errstr;
	$proxynode->{_dbh_proxy}->do("LOAD MYSQL SERVERS TO RUNTIME") or die "Couldn't execute statement: " .  $proxynode->{_dbh_proxy}->errstr;
	print Utils->print_log(2," Move node:" 
	    ." SQL:" .$proxy_sql_command
	    ."\n" );			    
	
	
    }

}

{
    package ProxySqlHG;
    sub new {
        my $class = shift;
        
        my $self = {
            _id  => undef, # 
            _type  => undef, # available types: w writer; r reader ; mw maintance writer; mr maintenance reader
	    _size => 0,
        };
        bless $self, $class;
        return $self;
    }
    
    sub id {
        my ( $self, $id ) = @_;
        $self->{_id} = $id if defined($id);
        return $self->{_id};
    }

    sub type {
        my ( $self, $type ) = @_;
        $self->{_type} = $type if defined($type);
        return $self->{_type};
    }
    sub size {
        my ( $self, $size ) = @_;
        $self->{_size} = $size if defined($size);
        return $self->{_size};
    }

}

{
    package Utils;
    use Time::HiRes qw(gettimeofday);
    #============================================================================
    ## get_connection -- return a valid database connection handle (or die)
    ## $dsn  -- a perl DSN, e.g. "DBI:mysql:host=ltsdbwm1;port=3311"
    ## $user -- a valid username, e.g. "check"
    ## $pass -- a matching password, e.g. "g33k!"
    
    sub get_connection($$$$) {
      my $dsn  = shift;
      my $user = shift;
      my $pass = shift;
      my $SPACER = shift;
      my $dbh = DBI->connect($dsn, $user, $pass);
    
      if (!defined($dbh)) {
        print Utils->get_current_time ."[ERROR] Cannot connect to $dsn as $user\n";
#        die();
	return undef;
      }
      
      return $dbh;
    }
    
    
    #Prrint time from invocation with milliseconds
    sub get_current_time{
	use POSIX qw(strftime);
	my $t = gettimeofday();
	my $date = strftime "%Y/%m/%d %H:%M:%S", localtime $t;
	$date .= sprintf ".%03d", ($t-int($t))*1000; # without rounding
	
	return $date;
    }

    #prit all environmnt variables    
    sub debugEnv{
        my $key = keys %ENV;
        foreach $key (sort(keys %ENV)) {
    
           print $key, '=', $ENV{$key}, "\n";
    
        }
    
    }
    
    
    #Print a log entry
    sub print_log($$){
	my $log_level = $_[1];
	my $text = $_[2];
	my $log_text = "[ - ] ";
	
	    SWITCH: {
		if ($log_level == 1) { $log_text= "[ERROR] "; last SWITCH; }
                if ($log_level == 2) { $log_text= "[WARN] "; last SWITCH; }
                if ($log_level == 3) { $log_text= "[INFO] "; last SWITCH; }
		if ($log_level == 4) { $log_text= "[DEBUG] "; last SWITCH; }
            }
	return Utils::get_current_time.":".$log_text.$text;
	
    }
    
    
    #trim a string
    sub  trim {
        my $s = shift;
        $s =~ s/^\s+|\s+$//g;
        return $s
    };


}


# ############################################################################
# Documentation
# #################
=pod

=head1 NAME
server_monitor.pl

=head1 OPTIONS
server_monitor.pl -u=admin -p=admin -h=192.168.1.50 -G=601 -P=3310 --debug=0  --log <full_path_to_file> --help
sample [options] [file ...]
 Options:
   -u|user            user to connect to the proxy
   -p|password        Password for the proxy
   -h|host            Proxy host
   -G                 Hostgroup ID with role definition. 
   --retry_down       The number of loop/test the check has to do before moving a node Down (default 0)
   --debug
   --log	      Full path to the log file ie (/var/log/proxysql/galera_check_) the check will add
		      the identifier for the specific HG.
   -help              help message
   
=head1 DESCRIPTION

Server monitor is a script to remove the server from the HostGroup which are not part of the replicaset anymore.
The script monitors the read_only flag and the repl_lag. If read_only=1 and repl_lag is NULL it removes the server from the HostGroup.

Why we need this?

Example: If we promote a new master and the old master won't be the a slave of the ne master, the ProxySQL will thought that server now is part of the
read hostgroup and going to send reads. But the old master does not part of the replicaset anymore. ProxySQL should not send any traffic.

=head1 Configure in ProxySQL


INSERT  INTO scheduler (id,interval_ms,filename,arg1) values (10,2000,"/var/lib/proxysql/server_monitor.pl","-u=admin -p=admin -h=192.168.1.50 -G=601 -P=3310 --retry_down=2 --debug=0  --log=/var/lib/proxysql/server_check");
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;
  
update scheduler set arg1="-u=admin -p=admin -h=192.168.1.50 -G=601 -P=3310 --debug=1  --log=/var/lib/proxysql/server_check" where id =10;  
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;

delete from scheduler where id=10;
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;


=cut	

