#!/bin/sh

# This reads output from apcaccess and prints it in a form that can be parsed by
# the collector command. It has been tested with apcupsd 3.14.10-2 and a
# CyberPower CP685AVR UPS.

apcaccess | sed -nre '
s/^BCHARGE\s+:\s+([0-9.]+).*/battery_percent \1/p
s/^LINEV\s+:\s+([0-9.]+).*/line_voltage \1/p
s/^LOADPCT\s+:\s+([0-9.]+).*/load_percent \1/p
s/^STATUS\s+:\s+ONBATT/on_line 0/p
s/^STATUS\s+:\s+ONLINE/on_line 1/p
'
