#!/usr/bin/env python

import os, sys, glob, time, datetime, subprocess

from scxkw.config import GEN2HOST, GEN2PATH_NODELETE, GEN2PATH_OKDELETE, CAMIDS
from scxkw.redisutil.typed_db import Redis

from g2base.remoteObjects import remoteObjects as ro

from astropy.io import fits

import shutil


def gen2_getframeids(g2proxy, camcode, nfrmids):
    '''
        Utility function - get <nfrmids> frameIDs from gen2 for 1 given <camcode> ('B', 'C', etc)
    '''

    # want to allocate some frames.
    g2proxy.executeCmd('SCEXAO', 'foo', 'get_frames', camcode,
                       {'num': nfrmids})

    # frames will be stored one per line in /tmp/frames.txt
    # We need to wait for gen2 to push the file

    ids_filename = f"/tmp/frames_{camcode}.txt"
    while True:
        time.sleep(0.01)
        try:
            with open(ids_filename, 'r') as in_f:
                # give it more time if the file is being written, to avoid a race upon read...
                time.sleep(0.1)
                frames = in_f.read().split('\n')
            if len(frames) > 0:
                os.remove(ids_filename)
                break
        except FileNotFoundError:
            pass

    return frames


def archive_monitor_process_filename(raw_file_list,
                                     stream_name_from_folder=True):
    '''
        Utility function - process a list of files

        raw_file_list: list of full path to files, e.g.
            ['/mnt/tier1/20220304/kcam/kcam_1234123412341234.fits', etc etc]
                                   ^---  ^----------
                                      |            |
        stream_name_from_folder: get this kcam or that kcam in the full file name

        Returns:
            file_list: the original list ***filtered to only valid archivable stream_names***
            file_list_shortname: n_files list of what's after the last /
            stream_names: n_files list of stream names
            dates: n_files list of the date string from the full filename
    '''
    # Isolate stream names and dates
    if stream_name_from_folder:
        # From the last folder name before the filename
        stream_names = [fullname.split('/')[-2] for fullname in raw_file_list]
    else:
        # Get it from the first substring of the fits file name
        stream_names = [
            fullname.split('/')[-1].split('_')[0] for fullname in raw_file_list
        ]
    # Discard non-ok stream names !
    # This is important if files that we're not allowed to archive end in the
    # ARCHIVED_DATA folder
    file_list = [
        f for (f, s) in zip(raw_file_list, stream_names) if s in CAMIDS
    ]
    stream_names = [s for s in stream_names if s in CAMIDS]

    file_list_shortname = [fullname.split('/')[-1] for fullname in file_list]
    dates = [fullname.split('/')[-3] for fullname in file_list]

    return file_list, file_list_shortname, stream_names, dates


def archive_monitor_push_files(g2proxy_obj,
                               n_files_max=None,
                               *,
                               skip_last_wait=True,
                               time_allowed=(1080, 1380),
                               bandwidth_mbps_assumed=800):

    '''
        Macro function: push .fits.fz files in GEN2_NODELETE path to gen2.

        Look for SCX*.fits.fz files - check the compression job is not ongoing.
        Request a push to gen2.
        Sleep assuming a given bandwidth.

        Already requested files are stored in archive_requested.txt folder in each repo

        Behold ! It does not mean the g2 request will be executed right away.
        We need to wait for STARS feedback to allow deletion.
    '''

    if time_allowed is None:
        time_allowed = (0, 1440)
    time_start, time_stop = time_allowed

    # Is this allowed to run now?
    current_time = datetime.datetime.now()
    t_minutes = current_time.hour * 60 + current_time.minute
    if (((time_stop >= time_start) and ((t_minutes < time_start) or
                                        (t_minutes >= time_stop)))
            or ((time_stop < time_start) and (t_minutes >= time_stop) and
                (t_minutes < time_start))):
        print(
            f'archive_monitor_push_files not allowed to run at this time ({time_start // 60}h{time_start % 60:02d}-{time_stop // 60}h{time_stop % 60:02d} UT)'
        )
        return

    fz_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits.fz')
    # Exclude running fpack jobs if any
    running_jobs = subprocess.run(
        'ps -eo args | grep fpack', shell=True,
        capture_output=True).stdout.decode('utf8').rstrip().split('\n')
    running_jobs = [
        job.split(' ')[-1] for job in running_jobs if job[-5:] == ".fits"
    ]  # Now this should be a full path!

    file_list = list(set(fz_file_list) - set(running_jobs))
    file_list.sort()

    (file_list, file_list_shortname, stream_names,
     dates) = archive_monitor_process_filename(file_list, True)

    if n_files_max is None or n_files_max > len(file_list):
        n_files_max = len(file_list)

    print(f'archive_monitor_push_files: {GEN2PATH_NODELETE}*/*/SCX*.fits.fz')
    print(
        f'archive_monitor_push_files: found {n_files_max} SCX*.fits.fz files...'
    )

    for ii in range(len(file_list)):

        if ii == n_files_max:
            break

        # Open the tracking file
        this_file_shortname = file_list_shortname[ii]
        archive_req_filename = GEN2PATH_NODELETE + f'/{dates[ii]}/{stream_names[ii]}/archive_requested.txt'
        try:
            with open(archive_req_filename, 'r') as archive_logfile:
                files_pushed = [
                    l.rstrip() for l in archive_logfile.readlines()
                ]
        except FileNotFoundError:
            files_pushed = []

        if this_file_shortname in files_pushed:  # File has been requested for push yet
            # We skip a file so we give us one more n_file_max
            n_files_max += 1
            continue

        # Get file size
        # Assume tranfer speed of 90 MB/s, only wait if it's not the last file.
        size_MB = os.stat(file_list[ii]).st_size / 1024 / 1024
        how_long = size_MB / (bandwidth_mbps_assumed / 8)

        # Send gen2 transfer query (remove .fits extension)
        g2proxy_obj.executeCmd('SCEXAO', 'foo', 'archive_fits', [], {
            'frame_no': this_file_shortname.split('.')[0],
            'path': file_list[ii]
        })
        print(f'Submitting {this_file_shortname} to gen2 archive.')
        # Note that the query has been issued
        with open(archive_req_filename, 'a') as archive_logfile:
            archive_logfile.write(this_file_shortname + '\n')

        if ii == n_files_max - 1 and skip_last_wait:
            print(
                f'Expected transfer time for last file is {how_long} sec from now ({size_MB} MB).'
            )
        else:
            print(
                f'Waiting {how_long} sec for file {ii+1}/{n_files_max} to transfer ({size_MB} MB).'
            )
            time.sleep(how_long)

        # Is this still allowed to run now?
        current_time = datetime.datetime.now()
        t_minutes = current_time.hour * 60 + current_time.minute
        if (((time_stop >= time_start) and ((t_minutes < time_start) or
                                            (t_minutes >= time_stop)))
                or ((time_stop < time_start) and (t_minutes >= time_stop) and
                    (t_minutes < time_start))):
            print(
                f"archive_monitor_push_files: aborting gen2 upload requests at {current_time.hour}:{current_time.minute}"
            )
            break


def archive_migrate_compressed_files(*, time_allowed=(1020, 1050)):
    '''
        Macro function

        Watches for SCX*.fits.fz files and moves the corresponding SCX*.fits from GEN2_NODELETE to GEN2_OKDELETE

        Is only allowed during certain times to avoid confusion during a night - default 7AM - 7:30AM = 17h UT - should be blazing fast.
    '''
    # Is this allowed to run now?
    time_start, time_stop = time_allowed
    current_time = datetime.datetime.now()
    t_minutes = current_time.hour * 60 + current_time.minute
    if (((time_stop >= time_start) and ((t_minutes < time_start) or
                                        (t_minutes >= time_stop)))
            or ((time_stop < time_start) and (t_minutes >= time_stop) and
                (t_minutes < time_start))):
        print(
            f'archive_migrate_compressed_files not allowed to run at this time ({time_start // 60}h{time_start % 60:02d}-{time_stop // 60}h{time_stop % 60:02d} UT)'
        )
        return

    # Process relevant file list: fz files exist and SCX file exists and no fpack job running
    scx_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits')
    fz_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits.fz')
    # Remove the fz extension from the fz files
    fz_file_list = [filename[:-3] for filename in fz_file_list]

    # Now check for active fpack jobs! We spawn fpack jobs with the full file path so we expect them as such
    running_jobs = subprocess.run(
        'ps -eo args | grep fpack', shell=True,
        capture_output=True).stdout.decode('utf8').rstrip().split('\n')
    running_jobs = [
        job.split(' ')[-1] for job in running_jobs if job[-5:] == ".fits"
    ]  # Now this should be a full path!

    file_list = list(
        set(fz_file_list).intersection(set(scx_file_list)) - set(running_jobs))
    file_list.sort()

    (file_list, file_list_shortname, stream_names,
     dates) = archive_monitor_process_filename(file_list, True)

    n_files = len(file_list)
    print(
        f'archive_migrate_compressed_files: found {n_files} SCX*.fits files to move...'
    )

    for ii in range(n_files):
        # Open the tracking file
        full_filename = file_list[ii]
        this_file_shortname = file_list_shortname[ii]

        try:
            os.makedirs(GEN2PATH_OKDELETE + f'/{dates[ii]}/{stream_names[ii]}')
        except FileExistsError:
            pass

        new_full_filename = GEN2PATH_OKDELETE + f'/{dates[ii]}/{stream_names[ii]}/{this_file_shortname}'

        # Move the file
        os.rename(full_filename, new_full_filename)

        ftxt_origin = '.'.join(full_filename.split('.')[:-1]) + '.txt'
        ftxt_destination = '.'.join(new_full_filename.split('.')[:-1]) + '.txt'
        # Move the txt file - if it's there.
        if os.path.exists(ftxt_origin):
            os.rename(ftxt_origin, ftxt_destination)

        # Carry the name_changes file - yes we're doing this a useless amount of times.
        shutil.copyfile(
            GEN2PATH_NODELETE +
            f'/{dates[ii]}/{stream_names[ii]}/name_changes.txt',
            GEN2PATH_OKDELETE +
            f'/{dates[ii]}/{stream_names[ii]}/name_changes.txt')

        # If no more fits in <date>/<stream>/ ?
        # Delete archive_requested.txt and name_changes.txt
        local_sublist = glob.glob(GEN2PATH_NODELETE +
                                  f'{dates[ii]}/{stream_names[ii]}/SCX*.fits')
        if len(local_sublist) == 0:
            os.remove(GEN2PATH_NODELETE +
                      f'/{dates[ii]}/{stream_names[ii]}/name_changes.txt')


def archive_monitor_compression(*, max_jobs=20):
    '''
        Macro function: watches for SCX*.fits files in GEN2_NODELETE and spawns
        fpack compression jobs.

        Manages a cap of max_jobs compression jobs.
    '''
    scx_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits')
    fz_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits.fz')
    # Remove the fz extension from the fz files
    fz_file_list = [filename[:-3] for filename in fz_file_list]

    # Now check for active fpack jobs! We spawn fpack jobs with the full file path so we expect them as such
    running_jobs = subprocess.run(
        'ps -eo args | grep fpack', shell=True,
        capture_output=True).stdout.decode('utf8').rstrip().split('\n')
    running_jobs = [
        job.split(' ')[-1] for job in running_jobs if job[-5:] == ".fits"
    ]  # Now this should be a full path!

    n_running_jobs = len(running_jobs)
    print(
        f'archive_monitor_compression: found {n_running_jobs} running fpack instances.'
    )

    file_list = list(
        set(scx_file_list) - set(fz_file_list) - set(running_jobs))
    file_list.sort()
    print(
        f'archive_monitor_compression: found {len(file_list)} SCX files to compress.'
    )

    # Spawn compression jobs
    for file_fullname in file_list[:max_jobs - n_running_jobs]:
        subprocess.run(f'fpack -h -s 0 -q 20 -v {file_fullname} &', shell=True)


def archive_monitor_get_ids(g2proxy_obj):
    '''
        Macro function: watches for *.fits files in GEN2_NODELETE and get a frameID for them
    '''

    # List and sort relevant files - expect GEN2PATH/date/stream/*.fits
    # Set difference to exclude already renamed files
    # It is important not to hit the .tmp files
    file_list = list(
        set(glob.glob(GEN2PATH_NODELETE + '*/*/*.fits')) -
        set(glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits')))
    file_list.sort()
    # FORMAT: file_list contains FULL PATH filenames

    (file_list, file_list_shortname, stream_names,
     dates) = archive_monitor_process_filename(file_list, False)

    # Dict to count how many IDs we need per "letter"
    per_id_count = {}
    id_current_count = {}  # See later - to assign to file
    for stream_name in CAMIDS:
        per_id_count[CAMIDS[stream_name]] = 0
        id_current_count[CAMIDS[stream_name]] = 0
    frame_ids = {}

    # Count files
    for sname in stream_names:
        per_id_count[CAMIDS[sname]] += 1

    if any(per_id_count.values()):
        print("archive_monitor_get_ids: frame ID requests: ", per_id_count)

    # Request file_ids
    for id_letter in per_id_count:
        if per_id_count[id_letter] > 0:
            frame_ids[id_letter] = gen2_getframeids(g2proxy_obj, id_letter,
                                                    per_id_count[id_letter])

    for ii in range(len(file_list)):
        date = dates[ii]
        sname = stream_names[ii]
        fname = file_list[ii]  # full path
        fname_short = file_list_shortname[ii]  # just file name
        letter = CAMIDS[stream_names[ii]]

        frame_id = frame_ids[letter][id_current_count[letter]]

        # Update the keyword with the FRAMEID
        # This could error?
        fits.setval(fname, "FRAMEID", value=frame_id, savecomment=True)
        fits.setval(fname,
                    "EXP-ID",
                    value=frame_id.replace(frame_id[3], "E", 1),
                    savecomment=True)

        # Maintain the file/id mapping text files (per date and stream)
        with open(GEN2PATH_NODELETE + f'/{date}/{sname}/name_changes.txt',
                  'a') as namelog:
            namelog.write(f"{fname_short}\t{frame_id}.fits\n")

        # Rename the files
        os.rename(fname,
                  GEN2PATH_NODELETE + f'/{date}/{sname}/{frame_id}.fits')
        os.rename('.'.join(fname.split('.')[:-1]) + '.txt',
                  GEN2PATH_NODELETE + f'/{date}/{sname}/{frame_id}.txt')

        id_current_count[letter] += 1

    # Done !

def archive_monitor_check_STARS_and_delete():
    '''
        Macro function:
        - list *.fits.fz files in GEN2_NODELETE
        - find the list that are receved in STARS
        - delete them
        - find empty folders
        - remove archive_requested.txt to leave folders actually empty
    '''
    pass


if __name__ == "__main__":

    # ------------------------------------------------------------------
    #                Configure communication with Gen2
    # ------------------------------------------------------------------

    # Do this once, and once only on process startup
    ro.init([GEN2HOST])

    g2proxy_obj = ro.remoteObjectProxy('SCEXAO')

    try:
        while True:
            archive_monitor_get_ids(g2proxy_obj)
            archive_monitor_push_files(g2proxy_obj, skip_last_wait=False)
            time.sleep(10.0)
    except KeyboardInterrupt:
        sys.exit(0)
