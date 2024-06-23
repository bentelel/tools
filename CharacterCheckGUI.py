import re
import sys
from pathlib import Path
from subprocess import check_call
from sys import executable
import concurrent.futures
import asyncio

import pandas


## does not run from desktop yet >> building wheels for webview fails..

# Function to install packages
def install(package):
    check_call([executable, "-m", "pip", "install", package])

try:
    import pandas as pd
except ImportError:
    install('pandas')
    import pandas as pd

try:
    import chardet
except ImportError:
    install('chardet')
    import chardet

try:
    import webview
except ImportError:
    #check_call([executable, "-m", "pip", "install", "--upgrade" ,"pip"])
    install('webview')
    import webview


try:
    from nicegui import ui, app
except ImportError:
    install('pywebview')
    install('nicegui')
    from nicegui import ui, app

# list with available encodings for pandas.read_csv().
available_encodings = ['ascii','big5','big5hkscs','cp037','cp273','cp424','cp437','cp500','cp720','cp737','cp775','cp850','cp852','cp855','cp856','cp857','cp858','cp860','cp861','cp862','cp863','cp864','cp865','cp866','cp869','cp874','cp875','cp932','cp949','cp950','cp1006','cp1026','cp1125','cp1140','cp1250','cp1251','cp1252','cp1253','cp1254','cp1255','cp1256','cp1257','cp1258','euc_jp','euc_jis_2004','euc_jisx0213','euc_kr','gb2312','gbk','gb18030','hz','iso2022_jp','iso2022_jp_1','iso2022_jp_2','iso2022_jp_2004','iso2022_jp_3','iso2022_jp_ext','iso2022_kr','latin_1','iso8859_2','iso8859_3','iso8859_4','iso8859_5','iso8859_6','iso8859_7','iso8859_8','iso8859_9','iso8859_10','iso8859_11','iso8859_13','iso8859_14','iso8859_15','iso8859_16','johab','koi8_r','koi8_t','koi8_u','kz1048','mac_cyrillic','mac_greek','mac_iceland','mac_latin2','mac_roman','mac_turkish','ptcp154','shift_jis','shift_jis_2004','shift_jisx0213','utf_32','utf_32_be','utf_32_le','utf_16','utf_16_be','utf_16_le','utf_7','utf_8','utf_8_sig']

characters_to_escape_in_regex = ['.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\']


class FileHandler:
    _instance = None

    def __init__(self, file_path, encoding):
        self.path = Path('/abc/123')
        self.dataframe = pd.DataFrame([])
        self.encoding = 'latin_1'
        self.check_char_user_input = ','
        self.check_chars = [',']
        self.check_chars_regex = ','
        self.cols_with_char = {}
        self.dataframe_length = 0

    def __new__(cls, file_path, encoding):
        if cls._instance is None:
            cls._instance = super(FileHandler, cls).__new__(cls)
        return cls._instance

    def set_check_char(self, character: str) -> None:
        self.check_char_user_input = character.encode(self.encoding)

    def set_file_path(self, file_path: str) -> None:
        if file_path:
            self.path = Path(file_path)
        else:
            self.path = Path('/abc/123')

    def set_dataframe_from_filepath(self) -> None:
        self.dataframe = pd.read_csv(self.path, encoding=self.encoding, low_memory=False)
        self.dataframe_length = len(self.dataframe)

    def set_encoding(self, encoding: str) -> None:
        if encoding not in available_encodings:
            self.encoding = 'latin_1'
        else:
            self.encoding = encoding.lower.replace("-","_")

    def update_check_values_and_regex(self) -> None:
        self.check_chars = list(set(self.check_char_user_input.split(" ")))
        # in case the user has a trailing space we get an empty string in the list, this removes that:
        if '' in self.check_chars:
            self.check_chars.remove('')
        new_chars = []
        for c in self.check_chars:
            if c in characters_to_escape_in_regex:
                c_new = re.escape(c)
                self.check_chars.remove(c)
                new_chars.append(c_new)
        self.check_chars += new_chars
        for c in self.check_chars:
            self.check_chars_regex = '|'.join(self.check_chars)

    async def analyze_dataframe(self) -> None:
        # for col in self.dataframe:
        #     filtered_df = self.dataframe[self.dataframe[col].astype(str).str.contains(self.check_char, na=False)]
        #     if not filtered_df.empty:
        #         self.cols_with_char[col] = len(filtered_df)
        # the above was my own code, the following is chat gpt. i could not get this func to work with io_bound or
        # cpu_bound from nicegui without it breaking on larger sets. this seems to work, but i am not sure wtf is going on
        self.cols_with_char = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            loop = asyncio.get_running_loop()
            for col in self.dataframe:
                filtered_df = await loop.run_in_executor(
                    executor,
                    lambda col=col: self.dataframe[
                        self.dataframe[col].astype(str).str.contains(self.check_chars_regex, na=False, regex=True)]
                )
                if not filtered_df.empty:
                    len_df = len(filtered_df)
                    self.cols_with_char[col] = (len_df, f"{len_df/self.dataframe_length * 100:.2f}%")

    def get_filtered_rows(self, column_name: str ,head: int =10) -> pandas.DataFrame:
        return self.dataframe[
            self.dataframe[column_name].astype(str).str.contains(self.check_chars_regex, na=False)].head(head)

async def load_file_and_set_dataframe() -> None:
    analyze_button.set_visibility(False)
    analyze_button.update()
    file_path = await choose_file()
    # i would like to display the loading spinner here, but its not working. idk why
    fileHandler.set_file_path(file_path[0])
    fileHandler.set_dataframe_from_filepath()
    analyze_button.set_visibility(True)
    analyze_button.update()
    path_label.text = str(fileHandler.path)
    loading_spinner.set_visibility(False)


async def choose_file() -> str:
    file_types = ('CSV Files (*.csv)', 'All files (*.*)') # for some reason i can not only use the CSV part here. idk..
    files = await app.native.main_window.create_file_dialog(webview.OPEN_DIALOG, allow_multiple=False, file_types=file_types)
    return files


def kill_script() -> None:
    sys.exit()


async def analyzer_click() -> None:
    try:
        result_table.set_visibility(False)
        loading_spinner.set_visibility(True)
        analyze_button.set_visibility(False)
        await fileHandler.analyze_dataframe()
        populate_result_table()
        loading_spinner.set_visibility(False)
        analyze_button.set_visibility(True)
    except Exception as e:
        ui.notify(e)


def populate_result_table() -> None:
    columns = [
        {'name': 'column', 'label': 'Column', 'field': 'column', 'required': True, 'align': 'left'},
        {'name': 'count', 'label': 'Count of char', 'field': 'count', 'required': True, 'align': 'left'},
        {'name': 'perc_of_rows', 'label': 'Percentage of rows', 'field': 'perc_of_rows', 'required': True, 'align': 'left'}
    ]
    rows = []
    for key, value in fileHandler.cols_with_char.items():
        rows.append({'column': key, 'count': value[0], 'perc_of_rows': value[1]})
    result_table.columns = columns
    result_table.rows = rows
    result_table.update()
    result_table.set_visibility(True)


def show_data_rows(col_name: str) -> None:
    data_label.text = col_name
    filtered_df = fileHandler.get_filtered_rows(col_name, 10)
    data_table.columns = [{'name': col, 'label': col, 'field': col} for col in filtered_df.columns]
    data_table.rows = filtered_df.to_dict('records')
    data_table.update()
    data_table.set_visibility(True)
    panels.set_value('Data Preview')
    panels.update()



if __name__ in ("__main__", "__mp_main__"):
    app.on_shutdown(kill_script)  # sys exit is triggered here, for some reason that does not cleanly exit the script.. the script is exited by crashing though
    fileHandler = FileHandler('', '')

    # this smells to me and is a spaghetti. i need to refactor this to be more succinct.
    with ui.tabs().classes('w-full') as tabs:
        main_page = ui.tab('Main Tab')
        data_view = ui.tab('Data Preview')
    with ui.tab_panels(tabs, value=main_page).classes('w-full') as panels:
        with ui.tab_panel(main_page):
            ui.select(available_encodings, label='File encoding', with_input=True, value='latin_1').bind_value(
                fileHandler, 'encoding')
            ui.button('choose file', on_click=load_file_and_set_dataframe)

            with ui.expansion('file:', value=True).classes('w-full'):
                path_label = ui.label('--no file chosen--')

            # how can we make this so that we also update the value list and regex when this is changed
            check_character_input = ui.input(label='Character to check for (multiple divided by spaces)', value=',')
            check_character_input.bind_value(fileHandler, 'check_char_user_input')
            check_character_input.on_value_change(fileHandler.update_check_values_and_regex)


            analyze_button = ui.button('analyze file', on_click=analyzer_click)
            analyze_button.set_visibility(False)

            loading_spinner = ui.spinner(size='lg')
            loading_spinner.set_visibility(False)

            result_table = ui.table(columns=[], rows=[])
            result_table.add_slot('body-cell-title', r'<td><a :href="props.row.url">{{ props.row.title }}</a></td>')
            result_table.on('rowClick', lambda e: show_data_rows(e.args[1]["column"]))
            result_table.set_visibility(False)
        with ui.tab_panel(data_view):
            data_label = ui.label('')
            data_table = ui.table(columns=[], rows=[])
            data_table.set_visibility(False)

    ui.run(native=True, dark=True, reload=False, title='csv character analyzer', window_size=(500,800))