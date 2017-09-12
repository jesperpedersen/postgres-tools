#!/bin/sh
#
# The MIT License (MIT)
#
# Copyright (c) 2017 Jesper Pedersen <jesper.pedersen@comcast.net>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the Software
# is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

SECONDS=0
DATE=`date +"%Y%m%d"`
CLIENTS="1 10 25 50 75 100 125 150 175 200 250 300 350 375"
TCP=0
HOST=localhost
PORT=5432
SCALE=3000
TIME=180
PGSQL_ROOT=/opt/postgresql-11.x
PGSQL_DATA=/mnt/data/11.x
PGSQL_XLOG=/mnt/xlog/11.x
COMPILE=1
COMPILE_OPTIONS="--with-openssl --with-gssapi --enable-debug --enable-depend"
COMPILE_JOBS=60
CONFIGURATION=/home/postgres/Configuration/11.x
PROFILE_OFF_LOGGED=1
PROFILE_OFF_UNLOGGED=1
PROFILE_ON_LOGGED=1
PROFILE_ON_UNLOGGED=1
RUN_ONEPC=1
RUN_ONEPCP=1
RUN_RO=1
RUN_ROP=0
RUN_TWOPC=1
RUN_SSUP=0

function postgresql_start()
{
    $PGSQL_ROOT/bin/pg_ctl -D $PGSQL_DATA -l $PGSQL_DATA/logfile start >> $DATE-$HEAD-build.log
    sleep 5
}

function postgresql_stop()
{
    $PGSQL_ROOT/bin/pg_ctl -D $PGSQL_DATA -l $PGSQL_DATA/logfile stop
    sleep 5
}

function postgresql_configuration()
{
    rm -Rf $PGSQL_DATA/* $PGSQL_XLOG/*
    $PGSQL_ROOT/bin/initdb -D $PGSQL_DATA -X $PGSQL_XLOG >> $DATE-$HEAD-build.log
    cp $CONFIGURATION/* $PGSQL_DATA
}

function postgresql_synchronous_commit()
{
    sed -i 's/synchronous_commit = off/synchronous_commit = on/g' $PGSQL_DATA/postgresql.conf
}

function postgresql_compile()
{
    cd postgres

    HEAD=`git rev-parse HEAD`

    touch ../$DATE-$HEAD-build.log
    export CFLAGS="-O -fno-omit-frame-pointer" && ./configure --prefix $PGSQL_ROOT $COMPILE_OPTIONS >> ../$DATE-$HEAD-build.log
    make clean >> ../$DATE-$HEAD-build.log
    make -j $COMPILE_JOBS >> ../$DATE-$HEAD-build.log
    make install >> ../$DATE-$HEAD-build.log
    cd ..
}

function pgbench_init_logged()
{
    if [ "$TCP" = "1" ]; then
        $PGSQL_ROOT/bin/createdb -h $HOST -p $PORT -E UTF8 pgbench >> $DATE-$HEAD-build.log
        $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -i -s $SCALE -q pgbench >> $DATE-$HEAD-build.log
    else
        $PGSQL_ROOT/bin/createdb -E UTF8 pgbench >> $DATE-$HEAD-build.log
        $PGSQL_ROOT/bin/pgbench -i -s $SCALE -q pgbench >> $DATE-$HEAD-build.log
    fi
}

function pgbench_init_unlogged()
{
    if [ "$TCP" = "1" ]; then
        $PGSQL_ROOT/bin/createdb -h $HOST -p $PORT -E UTF8 pgbench >> $DATE-$HEAD-build.log
        $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -i -s $SCALE -q --unlogged-tables pgbench >> $DATE-$HEAD-build.log
    else
        $PGSQL_ROOT/bin/createdb -E UTF8 pgbench >> $DATE-$HEAD-build.log
        $PGSQL_ROOT/bin/pgbench -i -s $SCALE -q --unlogged-tables pgbench >> $DATE-$HEAD-build.log
    fi
}

function pgbench_1pc_standard()
{
    local FILE=$DATE-$HEAD-$1-$2-1pc-standard.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

function pgbench_1pc_prepared()
{
    local FILE=$DATE-$HEAD-$1-$2-1pc-prepared.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -M prepared -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -M prepared -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

function pgbench_readonly_standard()
{
    local FILE=$DATE-$HEAD-$1-$2-readonly.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -S -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -S -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

function pgbench_readonly_prepared()
{
    local FILE=$DATE-$HEAD-$1-$2-readonly-prepared.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -M prepared -S -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -M prepared -S -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

function pgbench_2pc_standard()
{
    local FILE=$DATE-$HEAD-$1-$2-2pc-standard.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -X -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -X -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

function pgbench_skipsomeupdates_prepared()
{
    local FILE=$DATE-$HEAD-$1-$2-ssu-prepared.txt
    touch $FILE
    for i in $CLIENTS; do
        echo "DATA "$i >> $FILE
        if [ "$TCP" = "1" ]; then
            $PGSQL_ROOT/bin/pgbench -h $HOST -p $PORT -c $i -j $i -M prepared -N -T $TIME -U postgres pgbench >> $FILE
        else
            $PGSQL_ROOT/bin/pgbench -c $i -j $i -M prepared -N -T $TIME -U postgres pgbench >> $FILE
        fi
        echo "" >> $FILE
    done
}

postgresql_stop

# Compile
if [ $COMPILE == 1 ]; then
    postgresql_compile
else
    if [ -z "$1" ]; then
        HEAD=unknown
    else
        HEAD=$1
    fi
fi

# Off / Logged
if [ $PROFILE_OFF_LOGGED == 1 ]; then
    if [ $RUN_ONEPC == 1 ]; then
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_1pc_standard "off" "logged"
    fi

    if [ $RUN_ONEPCP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_1pc_prepared "off" "logged"
    fi

    if [ $RUN_RO == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_readonly_standard "off" "logged"
    fi

    if [ $RUN_ROP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_readonly_prepared "off" "logged"
    fi

    if [ $RUN_TWOPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_2pc_standard "off" "logged"
    fi

    if [ $RUN_SSUP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_logged
        pgbench_skipsomeupdates_prepared "off" "logged"
    fi
fi

# Off / Unlogged
if [ $PROFILE_OFF_UNLOGGED == 1 ]; then
    if [ $RUN_ONEPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_1pc_standard "off" "unlogged"
    fi

    if [ $RUN_ONEPCP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_1pc_prepared "off" "unlogged"
    fi

    if [ $RUN_RO == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_readonly_standard "off" "unlogged"
    fi

    if [ $RUN_ROP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_readonly_prepared "off" "unlogged"
    fi

    if [ $RUN_TWOPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_2pc_standard "off" "unlogged"
    fi

    if [ $RUN_SSUP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_start
        pgbench_init_unlogged
        pgbench_skipsomeupdates_prepared "off" "unlogged"
    fi
fi

# On / Logged
if [ $PROFILE_ON_LOGGED == 1 ]; then
    if [ $RUN_ONEPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_1pc_standard "on" "logged"
    fi

    if [ $RUN_ONEPCP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_1pc_prepared "on" "logged"
    fi

    if [ $RUN_RO == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_readonly_standard "on" "logged"
    fi

    if [ $RUN_ROP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_readonly_prepared "on" "logged"
    fi

    if [ $RUN_TWOPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_2pc_standard "on" "logged"
    fi

    if [ $RUN_SSUP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_logged
        pgbench_skipsomeupdates_prepared "on" "logged"
    fi
fi

# On / Unlogged
if [ $PROFILE_ON_UNLOGGED == 1 ]; then
    if [ $RUN_ONEPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_1pc_standard "on" "unlogged"
    fi

    if [ $RUN_ONEPCP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_1pc_prepared "on" "unlogged"
    fi

    if [ $RUN_RO == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_readonly_standard "on" "unlogged"
    fi

    if [ $RUN_ROP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_readonly_prepared "on" "unlogged"
    fi

    if [ $RUN_TWOPC == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_2pc_standard "on" "unlogged"
    fi

    if [ $RUN_SSUP == 1 ]; then
        postgresql_stop
        postgresql_configuration
        postgresql_synchronous_commit
        postgresql_start
        pgbench_init_unlogged
        pgbench_skipsomeupdates_prepared "on" "unlogged"
    fi
fi

# PostgreSQL configuration
cp $CONFIGURATION/postgresql.conf $DATE-$HEAD-postgresql-conf.txt

# Environment
cat /etc/system-release > $DATE-$HEAD-environment.txt
cat /proc/version >> $DATE-$HEAD-environment.txt
gcc --version >> $DATE-$HEAD-environment.txt

# Build time
echo "Seconds: "$SECONDS >> $DATE-$HEAD-build.log
