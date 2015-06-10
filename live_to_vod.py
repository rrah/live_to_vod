"""
Author: Robert Walker
"""
import os
import json
import psycopg2
import psycopg2.extensions
import subprocess
import shutil
import time
import argparse


# Add all the command line arguments
parser = argparse.ArgumentParser(description = 'Take some HLS fragments and make a video')
parser.add_argument('title', action = 'store',
                    help = 'Title of the show')
parser.add_argument('vbid', action = 'store', 
                    help = 'ID of the video box this video belongs to')
parser.add_argument('-s', action = 'store', 
                    dest = 'start_time',
                    help = 'Start time (in seconds from epoch) of event')
parser.add_argument('-e',  action = 'store', 
                    dest = 'end_time',
                    help = 'End time (in seconds from epoch) of event')
parser.add_argument('-c', metavar = 'config', action = 'store', 
                    dest = 'config_file',
                    help = 'Configuration file for show information')
parser.add_argument('--frag_loc', metavar = 'dir', action = 'store', 
                    dest = 'fragments_location', default = '/data/webs/hls/www/hls/',
                    help = 'Where the fragments are located')
parser.add_argument('--finished_shows', metavar = 'dir', action = 'store', 
                    dest = 'finished_shows', default = '/mnt/Finished Shows/',
                    help = 'Location of finished shows drive')
parser.add_argument('--playout', metavar = 'dir', action = 'store', 
                    dest = 'playout', default = '/data/videos/web/playout/',
                    help = 'Location of playout storage')
parser.add_argument('--web_vod', metavar = 'dir', action = 'store', 
                    dest = 'web_vod', default = '/data/videos/web/14-15/',
                    help = 'Location of web VoD storage')
parser.add_argument('--hqdownload', metavar = 'dir', action = 'store',
                    dest = 'hqdownload', default = '/data/videos/web/HQdownload',
                    help = 'Location of HQDownload storage')
parser.add_argument('--hddownload', metavar = 'dir', action = 'store',
                    dest = 'hddownload', default = '/data/videos/web/HDdownload',
                    help = 'Location of HDDownload storage')
parser.add_argument('--temp_dir', metavar = 'dir', action = 'store',
                    dest = 'temp_dir', default = '/data/tmp',
                    help = 'Location for temporary files')
parser.add_argument('--production', metavar = 'string', action = 'store',
                    dest = 'production', default = None,
                    help = 'Name of production this show is part of')
parser.add_argument('--dbconf', metavar = 'file', help = 'Configuartion file for connecting to the database',
                    action = 'store', dest = 'dbconf', default = 'conf.json')
args = parser.parse_args()


# Set local variables to arguments
config_file         = args.config_file
title               = args.title
video_box_id        = args.vbid
fragments_location  = args.fragments_location
finished_shows      = args.finished_shows
web_vod             = args.web_vod
playout             = args.playout
hddownload          = args.hddownload
hqdownload          = args.hqdownload
temp_dir            = args.temp_dir
dbconf              = args.dbconf


conn = psycopg2.connect(**dbconf)
cur = conn.cursor()

# Load config file
with open(config_file) as config_file:
    config = json.load(config_file)


def get_stream():
    
    """
    Get the stream and program out of config file"""
    
    for stream in config:
        for program in config[stream]:
            if program['title'] == title:
                return program, stream


def cat_hls(file_location, start_time, end_time, file_name):
    
    """
    Cat the hls fragments together and pass to FFmpeg
    to repackage."""
    
    remove_list = ['index.m3u8', 'index.m3u8.bak', 'ob3_720', 'ob3_576',
                   'ob3_1080', 'ob3_480']
    file_list = os.listdir(file_location)
    file_list.remove('index.m3u8')
    
    for file_ in remove_list:
        try:
            file_list.remove(file_)
        except ValueError:
            pass
    cat_list = []
    file_list = [file_.rstrip('.ts') for file_ in file_list]
    file_list = [int(file_) for file_ in file_list]
    for file_ in sorted(file_list):
        if (file_ >= start_time * 1000 - 10000 
            and file_ <= end_time * 1000 + 10000):
            cat_list.append(file_location + str(file_) + '.ts')
    subprocess.call(['cat ' + ' '.join(cat_list) + ' > /data/tmp/' + 
                     file_name.rstrip('.mp4')], shell = True)
    subprocess.call(
        ['ffmpeg -y -i /data/tmp/{} -vcodec copy -acodec copy -bsf:a aac_adtstoasc /data/tmp/{}'.format(
                                    file_name.rstrip('.mp4'), file_name)], shell = True)
    
    return '/data/tmp/' + file_name


def move_file(source_location, destination_location):
    
    shutil.copy(source_location, destination_location)
    

def update_db_video(program):
    
    title           = program['title'].replace(':', '').replace('\'', '\'\'')
    display_name    = title
    url_name        = title.replace(' ', '_').replace('\'', '')
    created_date    = time.time()
    created_by      = 1952
    
    query = "INSERT INTO videos (video_box_id, display_name, url_name, created_date, created_by) VALUES ({},'{}','{}',{},{}) RETURNING id".format(video_box_id, display_name, url_name,  psycopg2.TimestampFromTicks(created_date), created_by)
    cur.execute(query)
    return int(cur.fetchall()[0][0])
    
def update_db_file(video_id, type_, filename):
    
    video_id                = video_id
    video_file_type_name    = type_
    filename                = filename
    
    query = "INSERT INTO video_files (video_id, video_file_type_name, filename) VALUES ({}, '{}', '{}') RETURNING id".format(video_id, video_file_type_name, filename)
    cur.execute(query)
    return int(cur.fetchall()[0][0])
 
def update_db_encode_job(source_file, destination_file, video_file_id, 
                         format_id, status = 'Not Encoding'):

    source_file         = source_file
    destination_file    = destination_file
    video_file_id       = video_file_id
    format_id           = format_id
    status              = status
    user_id             = 1952
    priority            = 5
    
    query = "INSERT INTO encode_jobs (source_file, destination_file, video_id, format_id, status, user_id, priority) VALUES ('{}', '{}', {}, {}, '{}', {}, {})".format(source_file, destination_file, video_file_id, format_id, status, user_id, priority)
    cur.execute(query)

def get_file_name(program, hd = ''):
    return '15_{}_sum05{}.mp4'.format(program['title'].replace(' ', '_').replace(':','').replace('\'',''), hd)


# Get stream details for wanted program
program, stream = get_stream()

# Work out if HD or SD
if stream == 'ob3':
    qualities = ['576', '360']
else:
    qualities = ['1080', '720', '360']
    

# Set the name of the output file and input file
location = fragments_location + stream + '_{}/'

# Get start/end times
if program['accurate_start'] == None:
    start_time = program['scheduled_start']
else:
    start_time = program['accurate_start']
    
if program['accurate_end'] == None:
    end_time = program['scheduled_end']
else:
    end_time = program['accurate_end']

# Do the things
video_id = update_db_video(program)
for quality in qualities:
    if quality == '1080':
    
        source_location = cat_hls(location.format(quality), 
                                  start_time, end_time, 
                                  get_file_name(program, '_' + quality))
        
        # Finished shows, so no DB
        move_file(source_location, finished_shows + get_file_name(program))
        
    elif quality == '720':
        
        source_location = cat_hls(location.format(quality), start_time, 
                                  end_time, get_file_name(program, 
                                                          '_' + quality))
        
        # Web
        move_file(source_location, web_vod + get_file_name(program, '_HD'))
        video_file_id = update_db_file(video_id, 'Online-HD mp4', 
                                       '14-15/' + get_file_name(program, '_HD'))
        update_db_encode_job(finished_shows + get_file_name(program), 
                             web_vod + get_file_name(program, '_HD'), 
                             video_file_id, 13, 'Done')
        
        # Playout
        move_file(source_location, playout + get_file_name(program))
        video_file_id = update_db_file(video_id, '720p Vidserv Video', 
                                       'playout/' + get_file_name(program))
        update_db_encode_job(finished_shows + get_file_name(program), 
                             hddownload + get_file_name(program), 
                             video_file_id, 7, 'Done')
        
        # Download
        video_file_id = update_db_file(video_id, 'HD download video', 
                                       'HDdownload/' + get_file_name(program))
        update_db_encode_job(finished_shows + get_file_name(program), 
                             hddownload + get_file_name(program), 
                             video_file_id, 10)
        
    elif quality == '576':
        
        source_location = cat_hls(location.format(quality), start_time, 
                                  end_time, get_file_name(program, 
                                                          '_' + quality))
        
        # Finished shows, so no DB
        move_file(source_location, finished_shows + get_file_name(program))
        
        # Playout
        move_file(source_location, playout + get_file_name(program))
        video_file_id = update_db_file(video_id, 'Wide Vidserv Video', 
                                       'playout/' + get_file_name(program))
        update_db_encode_job(finished_shows + get_file_name(program), 
                             playout + get_file_name(program), 
                             video_file_id, 6, 'Done')
        
        
    elif quality == '360':
        
        source_location = cat_hls(location.format(quality), start_time, 
                                  end_time, get_file_name(program, 
                                                          '_' + quality))
        
        if stream == 'ob3':
            # Download
            video_file_id = update_db_file(video_id, 'iPod 640 Wide', 
                                           'HQdownload/' + get_file_name(program))
            update_db_encode_job(finished_shows + get_file_name(program), 
                                 hqdownload + get_file_name(program), 
                                 video_file_id, 9)
        
        # Web
        move_file(source_location, web_vod + get_file_name(program))
        video_file_id = update_db_file(video_id, 'Semi-HD mp4', 
                                       '14-15/' + get_file_name(program))
        update_db_encode_job(finished_shows + get_file_name(program), 
                             web_vod + get_file_name(program), 
                             video_file_id, 3, 'Done')
        
conn.commit()
conn.close()
