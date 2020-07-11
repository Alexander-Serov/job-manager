#!/bin/bash
squeue -u alserov
squeue -u alserov | grep -c R
squeue -u alserov | grep PD
