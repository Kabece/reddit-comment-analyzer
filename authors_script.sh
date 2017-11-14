#!/bin/sh
#PBS -q hpc
#PBS -l walltime=01:30:00
#PBS -l nodes=1:ppn=4
#PBS -N Authors
#PBS -l mem=16gb
#PBS -m abe

if test X$PBS_ENVIRONMENT = XPBS_BATCH; then cd $PBS_O_WORKDIR; fi

module load python3/3.4.1

python3 authors.py reddit.db