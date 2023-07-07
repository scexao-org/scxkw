#!/usr/bin/env python
from __future__ import annotations
import typing as typ

import logging

logg = logging.getLogger(__name__)

import os, sys, glob, time, datetime, subprocess

from scxkw.config import GEN2PATH_NODELETE, GEN2PATH_OKDELETE, CAMIDS, GEN2PATH_PRELIM
from scxkw.redisutil.typed_db import Redis
from scxkw.tools.compression_job_manager import FPackJobCodeEnum, FpackJobManager

from ..tools import file_tools
from ..tools.file_obj import FitsFileObj as FFO
from ..tools.vampires_synchro import VampiresSynchronizer

from g2base.remoteObjects import remoteObjects as ro

from astropy.io import fits

import shutil


def gen2_getframeids(g2proxy_scx, g2proxy_vmp, code: str, nfrmids: int) -> typ.List[str]:
    '''
        Utility function - get <nfrmids> frameIDs from gen2 for 1 given <camcode> ('SCXB', 'SCXC', 'VMPA', etc)
    '''

    # want to allocate some frames.
    assert len(code) == 4
    inst_code = code[:3]
    cam_code = code[3]
    inst = {'SCX': 'SCEXAO', 'VMP': 'VAMPIRES'}[inst_code]
    g2proxy = {'SCX': g2proxy_scx, 'VMP': g2proxy_vmp}[inst_code]

    g2proxy.executeCmd(inst, 'foo', 'get_frames', cam_code,
                       {'num': nfrmids})

    # frames will be stored one per line in /tmp/frames.txt
    # We need to wait for gen2 to push the file

    ids_filename = f"/tmp/frames_{cam_code}.txt"
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


def archive_monitor_process_filename(raw_file_list: typ.List[str],
                                     stream_name_from_folder: bool=True):
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


def archive_migrate_compressed_files(*, time_allowed=(17*60, 17*60 + 30)):
    '''
        Macro function

        Watches for SCX|VMP*.fits.fz files and moves the corresponding SCX|VMP*.fits from GEN2_NODELETE to GEN2_OKDELETE

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
    scx_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits') + glob.glob(GEN2PATH_NODELETE + '*/*/VMP*.fits')
    fz_file_list = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits.fz') + glob.glob(GEN2PATH_NODELETE + '*/*/VMP*.fits.fz')
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
        f'archive_migrate_compressed_files: found {n_files} (SCX|VMP)*.fits files to move...'
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



def synchronize_vampires_files(*, folder_root=GEN2PATH_NODELETE, sync_manager: VampiresSynchronizer):
    v1_fileobjs = file_tools.make_fileobjs_from_globs([folder_root + '/*/vcam1/*.fits'], [])
    v2_fileobjs = file_tools.make_fileobjs_from_globs([folder_root + '/*/vcam2/*.fits'], [])

    sync_manager.feed_file_objs(v1_fileobjs)
    sync_manager.feed_file_objs(v2_fileobjs)

    sync_manager.process_queues()


def archive_monitor_compression(*, job_manager: FpackJobManager):
    '''
        Macro function: watches for SCX*.fits files in GEN2_NODELETE and spawns
        fpack compression jobs.

        Manages a cap of max_jobs compression jobs.
    '''
    all_fits = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits') + glob.glob(GEN2PATH_NODELETE + '*/*/VMP*.fits')
    all_fzs = glob.glob(GEN2PATH_NODELETE + '*/*/SCX*.fits.fz') + glob.glob(GEN2PATH_NODELETE + '*/*/VMP*.fits.fz')
    

    # Note: for some of those files, the compression job may already be running!
    only_fits = file_tools.separate_compression_dups(all_fits, all_fzs)
    file_objs = file_tools.make_fileobjs_from_filenames(only_fits)


    # Cleanup:
    job_manager.refresh_running_jobs()
    # Spawn and count
    n_jobs = 0
    for file_obj in file_objs:
        ret = job_manager.run_fpack_compression_job(file_obj)
        if ret == FPackJobCodeEnum.STARTED:
            n_jobs += 1
        elif ret == FPackJobCodeEnum.TOOMANY:
            break
        # FPackJobCodeEnum.ALREADY_RUNNING continues silently

    print(f'archive_monitor_compression: '
          f'found {len(file_objs)} SCX/VMP files to compress; '
          f'started {n_jobs} fpacks.')

from scxkw.tools.pdi_deinterleave import deinterleave_filechecker, PDIDeintJobManager, PDIJobCodeEnum

def archive_monitor_deinterleave_or_passthrough(*, folder_root=GEN2PATH_NODELETE, job_manager: PDIDeintJobManager):
    # Allowed deinterleave streams and their target folder:
    PERMISSIBLE_STREAMS = {
        'apapane': 'agen2',
        'vsolo1': 'vgen2',
        'vsolo2': 'vgen2',
        'vsync': 'vgen2'
        }

    fileobj_list = file_tools.make_fileobjs_from_globs([folder_root + f'/*/{stream}/*.fits' for stream in PERMISSIBLE_STREAMS], [])
    fileobj_list = [f for f in fileobj_list if f.stream_from_foldername in PERMISSIBLE_STREAMS]

    needs_deinterleave = deinterleave_filechecker(fileobj_list)

    fileobj_need_deint = [fobj for b, fobj in zip(needs_deinterleave, fileobj_list) if b]
    fileobj_noneed_deint = [fobj for b, fobj in zip(needs_deinterleave, fileobj_list) if not b]

    job_manager.refresh_running_jobs()
    n_jobs = 0

    for file in fileobj_noneed_deint:
        stream = file.stream_from_foldername
        file.move_file_to_streamname(PERMISSIBLE_STREAMS[stream])

    for file in fileobj_need_deint:
        stream = file.stream_from_foldername

        ret = job_manager.run_pdi_deint_job(file, PERMISSIBLE_STREAMS[stream])
        if ret == PDIJobCodeEnum.STARTED:
            n_jobs += 1
        elif ret == PDIJobCodeEnum.TOOMANY:
            break
        # We silently pass on ALREADY_RUNNING and on STARTED.


def archive_monitor_get_ids(scx_proxy: ro.remoteObjectProxy,
                            vmp_proxy: ro.remoteObjectProxy):
    '''
        Macro function: watches for *.fits files in GEN2_NODELETE and get a frameID for them
    '''

    # List and sort relevant files - expect GEN2PATH/date/stream/*.fits
    # Set difference to exclude already renamed files
    # It is important not to hit the .tmp files
    fobj_list = file_tools.make_fileobjs_from_globs(
        [GEN2PATH_NODELETE + '*/agen2/*.fits', GEN2PATH_NODELETE + '*/vgen2/*.fits'],
        [GEN2PATH_NODELETE + '*/agen2/SCX*.fits', GEN2PATH_NODELETE + '*/vgen2/VMP*.fits']
    )
    assert all([not f.is_archived for f in fobj_list])
    assert all([not f.is_compressed for f in fobj_list])

    # Dict to count how many IDs we need per "letter"
    per_id_count = {}
    id_current_count = {}  # See later - to assign to file
    for stream_name in CAMIDS:
        per_id_count[CAMIDS[stream_name]] = 0
        id_current_count[CAMIDS[stream_name]] = 0
    frame_ids: typ.Dict[str, typ.List[str]] = {}

    # Count files

    for file in fobj_list:
        per_id_count[CAMIDS[file.stream_from_foldername]] += 1

    if any(per_id_count.values()):
        print("archive_monitor_get_ids: frame ID requests: ", per_id_count)

    # Request file_ids
    for id_letter in per_id_count:
        if per_id_count[id_letter] > 0:
            frame_ids[id_letter] = gen2_getframeids(scx_proxy, vmp_proxy, id_letter,
                                                    per_id_count[id_letter])

    for file in fobj_list:
        frame_id = frame_ids[CAMIDS[file.stream_from_foldername]].pop(0)

        # Update the keyword with the FRAMEID
        # This could error?
        with fits.open(file.full_filepath, "update") as hdul:
            header: fits.Header = hdul[0].header # get primary header
            header["FRAMEID"] = frame_id
            header["EXP-ID"] = frame_id.replace(frame_id[3], "E", 1)

        # Maintain the file/id mapping text files (per date and stream)
        with open(file.full_filepath.parent / 'name_changes.txt',
                  'a') as namelog:
            namelog.write(f"{file.file_name}\t{frame_id}.fits\n")

        # Rename the files
        file.rename_in_folder(frame_id + '.fits')

    # Done !
