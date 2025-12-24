import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/')) for x in ['src/excel/include']
]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['src/excel/excel_extension.cpp']]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'src/excel/xlsx/read_xlsx_metadata.cpp',
        'src/excel/xlsx/zip_file.cpp',
    ]
]
