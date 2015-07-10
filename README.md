## Overview

    rstream replicates a fileset at a source machine to multiple target
    destinations with updates in near realtime. It is optimised for 
    distributing incremental changes to files (e.g. log files).

    It's a bit like tail -f and rsync combined (although it cannot allude to
    any of the efficiencies that rsync offers).

    A rstream instance needs to run at source and destination. The instance
    at the source can distribute to multiple destinations, while the one at
    the destination can subscribe to updates from multiple sources.

## Usage 

    Server mode (run at source of data):
       rstream -l [args]

    Client mode (run at target destination):
       rstream [args] sourcehost1 ...
    
     Options:
      -l --listen         : run in server mode
      -P --port portnum   : port to listen on or connect to
      -d --directory path : root of shared fileset
      -r --regex pattern  : only share files with names matching this
      -s --stdout         : copy data to stdout (client mode)
      -z --compress       : use gzip compression for data in transit
      -c --checksum       : use SHA-1 digests to detect changes to files
                             Warning: slow for large files, or large numbers 
			     of files, or files that update frequently
      -h --help           : display this help
      -p --pidfile        : path to pid file, default is /var/run/rstream.pid
      -f --foreground     : run in foreground
      -v --verbose        : increase verbosity of log messages 
                             (specify multiple times to increase)

    
## License

    Please see the LICENSE file included with the source code of this project for the terms of the Artistic License under which this project is licensed. This license is also found on the web at: http://dev.perl.org/licenses/artistic.html

## 3rd Party packages not distributed with this project 

    Rstream uses several 3rd party open source libraries and tools.

    This section summarizes the tools used, their purpose, and the licenses under which they're released.


    * Digest::SHA v5.89 (Perl Artistic License)
    
    Used for calculating checksums on file contents.

    * IO::Compress::Gzip v2.064 (Perl Artistic License)

    Used for data compression.

    * String::Glob::Permute v0.1 (Perl Artistic License)

    Used for expanding hostnames according to patterns


