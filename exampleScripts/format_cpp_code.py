#!/usr/bin/env python

import argparse
import subprocess
import os
import os.path as path
import platform
import re
import sys

# root_dir is relative to the script (which is assumed to be in a tools directory) if you move this
# script, make sure to change the relative path to get back to the root
root_dir = .

clang_format_paths = {
    'Windows': 'Bin/win/clang-format.exe',
    'Linux': os.path.join(root_dir, 'tools/clang-format'),
    'Darwin': 'Bin/osx/clang-format'
}

clang_format_exe = clang_format_paths[platform.system()]

folders = [
    'applications',
    'drivers',
    'libs',
    'malleus',
    'ml_binder',
    'workloads',
    '../frameworks/libs/ml_pose_depot',
    '../frameworks/libs/totem_6dof_fusion',
    '../frameworks/libs/totem_types',
    '../frameworks/perception',
    '../tools/totem_6dof_predict',
    '../../nova/frameworks/services/perception_monitor',
]

exclude = [
    'libs/googletest',
    'libs/nanopb',
    'libs/headpose/pc/mlslam/opencv_lines',
    'libs/headpose/pc/mlslam/plsvo',
    'libs/headpose/pc/mlslam/poly_lines',
]

max_size = 256 * 1024

filetypes = [
    '.cc',
    '.cpp',
    '.h',
    '.hpp',
    '.inl'
]

def call(cmdline_args, **kwargs):
    process = subprocess.Popen(cmdline_args, stdout=subprocess.PIPE)
    out, err = process.communicate()
    return out

def is_included(file):
    file = file.replace('\\', '/')

    for f in folders:
        include_full = path.join(root_dir, f).replace('\\', '/')
        if (file.find(include_full) == 0):
            return True

    return False

def is_excluded(file):
    file = file.replace('\\', '/')

    for e in exclude:
        exclude_full = path.join(root_dir, e).replace('\\', '/')
        if (file.find(exclude_full) == 0):
            return True

    _, ext = path.splitext(file)
    if (not ext in filetypes):
        return True

    if os.path.getsize(file) > max_size:
        return True

    return False

def format_file(full_file, verify):
    if is_included(full_file) and not is_excluded(full_file):
        with open(full_file, 'r') as f:
            orig = f.read()

        if verify:
            new = call([clang_format_exe, full_file])
        else:
            call([clang_format_exe, '-i', full_file])
            with open(full_file, 'r') as f:
                new = f.read()
        if new != orig:
            if verify:
                print >> sys.stderr, "%s is not passing the coding standard."  % full_file[len(root_dir)+1:]
            else:
                print >> sys.stderr, "%s was modified"%full_file[len(root_dir)+1:]
            return False

    return True

PUSH_POP_RE = re.compile('mlx_compat/(push_warnings|pop_warnings)\.h')

if False:  # For debugging blank line insertion.
    blank_line_before = "// BLANK LINE BEFORE\n"
    blank_line_after = "// BLANK LINE AFTER\n"
else:
    blank_line_before = "\n"
    blank_line_after = "\n"

def fix_warning_guards_single_file(full_file, verify):
    if is_included(full_file) and not is_excluded(full_file):
        with open(full_file, 'r') as f:
            lines = f.readlines()
            changed = False

            line_num = 0
            while line_num + 1 < len(lines):
                line = lines[line_num]
                if PUSH_POP_RE.search(line):
                    if line_num > 0 and lines[line_num - 1].strip():
                        lines.insert(line_num, blank_line_before)
                        line_num += 1
                    if lines[line_num + 1].strip():
                        lines.insert(line_num + 1, blank_line_after)
                    changed = True
                line_num += 1

            if changed:
                if not verify:
                    tmp_path = full_file + '.tmp_fmt'
                    with open(tmp_path, 'w') as tmp_fp:
                        tmp_fp.writelines(lines)
                    os.rename(tmp_path, full_file)
                return False

    return True

def files_from_dir(dir_path):
    for root, dirs, files in os.walk(dir_path):
        if ".git" in dirs:
            dirs.remove(".git")
        for file_name in files:
            file_path = path.join(root, file_name)
            yield file_path

def files_from_list(path_list):
    for path in path_list:
        if os.path.isdir(path):
            for sub_path in files_from_dir(path):
                yield sub_path
        elif os.path.isfile(path):
            yield path

def parallel_map(function, args_generator):
    run_in_parallel = False
    try:
        import joblib
        import multiprocessing
        run_in_parallel = True
    except ImportError:
        print("Can't find the `joblib` library, will run in single-threaded mode.")
        print("To speed this up, run `sudo apt-get install python-joblib`")

    if run_in_parallel:
        return joblib.Parallel(n_jobs=multiprocessing.cpu_count())(
            joblib.delayed(function)(*args) for args in args_generator)
    else:
        return [function(*args) for args in args_generator]

def format_files(file_list, verify):
    print('Checking {} files...'.format(len(file_list)))
    return parallel_map(format_file, ((file_path, verify) for file_path in file_list))

def fix_warning_guards(file_list, verify):
    print('Checking {} files...'.format(len(file_list)))
    return parallel_map(fix_warning_guards_single_file, ((file_path, verify) for file_path in file_list))

def check_clang_format():
    process = subprocess.Popen([clang_format_exe], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, _ = process.communicate('void foo() { bar(); }')
    process.wait()

    if stdout != "void foo() {\n  bar();\n}":
        print('ERROR: clang-format is not working! Output:')
        print(stdout)
        sys.exit(1)

def main():
    "The main loop"
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", help="Verify the files instead of modifying them." , action="store_true")
    parser.add_argument("--verbosity", help="increase output verbosity.", action="store_true")
    parser.add_argument("--fix-warning-guards", help="ensure blank lines before/after (push|pop)_warnings.h #includes", action="store_true")
    parser.add_argument('--ignore-unenforced', help='only check files that are normally checked, even if the user provides other paths', action='store_true')
    parser.add_argument('path', nargs='*', type=str,
                        help='Path that need to be processed')
    args = parser.parse_args()
    if args.verbosity:
        print("verbosity turned on")

    check_clang_format()

    global folders
    if len(args.path)==0:
        args.path = folders

    path_list = [path.abspath(path.join(root_dir, sub_dir)) for sub_dir in args.path]

    if not args.ignore_unenforced:
        folders = path_list

    files_to_check = list(files_from_list(path_list))

    if args.fix_warning_guards:
        results = fix_warning_guards(files_to_check, args.verify)
    else:
        results = format_files(files_to_check, args.verify)

    ok = all(results)

    if ok:
        print >> sys.stderr, "No changes necessary."
    elif args.verify:
        print >> sys.stderr, "Please run %s without --verify to apply fixes."%__file__

    if not ok and args.verify:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main()

