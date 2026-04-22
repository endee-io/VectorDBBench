#!/bin/bash

SESSION_NAME="monitor"

# 1. Kill existing session to ensure a fresh layout every time you run the script
tmux kill-session -t $SESSION_NAME 2>/dev/null

# 2. Create the session
tmux new-session -d -s $SESSION_NAME

# 3. Layout Setup
# Split Top/Bottom
tmux split-window -v -t $SESSION_NAME:0
# Split Top into Left/Right
tmux select-pane -t $SESSION_NAME:0.0
tmux split-window -h -t $SESSION_NAME:0.0

# 4. Identify the NVMe device (Auto-detecting to avoid errors)
DISK=$(lsblk -dn -o NAME | grep nvme | head -n 1)
if [ -z "$DISK" ]; then DISK="sda"; fi # Fallback to sda if no NVMe found

# 5. Send commands
# Top-Left: nmon with CPU, Mem, Disk, and Net views active
tmux send-keys -t $SESSION_NAME:0.0 "nmon" C-m
sleep 0.5
tmux send-keys -t $SESSION_NAME:0.0 "cmdn"
# Top-Right: htop
tmux send-keys -t $SESSION_NAME:0.1 "htop" C-m
# Bottom: iostat (Using the detected disk)
tmux send-keys -t $SESSION_NAME:0.2 "iostat -xmty 1 /dev/$DISK" C-m

# 6. Attach or Switch
if [ -z "$TMUX" ]; then
    tmux attach-session -t $SESSION_NAME
else
    tmux switch-client -t $SESSION_NAME
fi
