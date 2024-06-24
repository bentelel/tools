from re import escape, sub
from pathlib import Path
from subprocess import check_call
from sys import executable, exit
import concurrent.futures
from asyncio import sleep, get_running_loop
from datetime import datetime

## remember to save .pyw file for use on desktop!

# Function to install packages
def install(package):
    check_call([executable, "-m", "pip", "install", package])

try:
    # be more succinct here, what do we need from pandas?
    import pandas as pd
except ImportError:
    install('pandas')
    import pandas as pd

try:
    import webview
except ImportError:
    install('pywebview')
    import webview

try:
    from nicegui import ui, app
except ImportError:
    install('nicegui')
    from nicegui import ui, app

# list with available encodings for pandas.read_csv().
available_encodings = ['ascii','big5','big5hkscs','cp037','cp273','cp424','cp437','cp500','cp720','cp737','cp775','cp850','cp852','cp855','cp856','cp857','cp858','cp860','cp861','cp862','cp863','cp864','cp865','cp866','cp869','cp874','cp875','cp932','cp949','cp950','cp1006','cp1026','cp1125','cp1140','cp1250','cp1251','cp1252','cp1253','cp1254','cp1255','cp1256','cp1257','cp1258','euc_jp','euc_jis_2004','euc_jisx0213','euc_kr','gb2312','gbk','gb18030','hz','iso2022_jp','iso2022_jp_1','iso2022_jp_2','iso2022_jp_2004','iso2022_jp_3','iso2022_jp_ext','iso2022_kr','latin_1','iso8859_2','iso8859_3','iso8859_4','iso8859_5','iso8859_6','iso8859_7','iso8859_8','iso8859_9','iso8859_10','iso8859_11','iso8859_13','iso8859_14','iso8859_15','iso8859_16','johab','koi8_r','koi8_t','koi8_u','kz1048','mac_cyrillic','mac_greek','mac_iceland','mac_latin2','mac_roman','mac_turkish','ptcp154','shift_jis','shift_jis_2004','shift_jisx0213','utf_32','utf_32_be','utf_32_le','utf_16','utf_16_be','utf_16_le','utf_7','utf_8','utf_8_sig']

characters_to_escape_in_regex = ['.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\']



class FileHandler:
    _instance = None

    def __init__(self, file_path, encoding):
        self.path = None # should probably change this to None, but maybe something breaks then?
        self.dataframe = pd.DataFrame([])
        self.encoding = 'latin_1'
        self.check_char_user_input = ','
        self.check_chars = [',']
        self.check_chars_regex = ','
        self.cols_with_char = {}
        self.dataframe_length = 0
        self.file_header = 0 # zero based row index!
        self.transformed_df = pd.DataFrame([])
        self.supress_unnamed_columns = True

    def __new__(cls, file_path, encoding):
        if cls._instance is None:
            cls._instance = super(FileHandler, cls).__new__(cls)
        return cls._instance

    def toggle_header_mode(self, event) -> None:
        if event:
            self.file_header = 0 # zero based row index!
        else:
            self.file_header = None

    def set_check_char(self, character: str) -> None:
        self.check_char_user_input = character.encode(self.encoding)

    def set_file_path(self, file_path: str) -> None:
        if file_path:
            self.path = Path(file_path)
        else:
            self.path = None

    def set_dataframe_from_filepath(self) -> None:
        # for some reason this does not work for .pyw files any longer..
        if self.supress_unnamed_columns:
            # usecols=lambda c: not c.startswith('Unnamed:') we use this to surpress unnamed cols in broken csvs
            self.dataframe = pd.read_csv(self.path, encoding=self.encoding, low_memory=False, header=self.file_header,
                                             dtype=str, na_values='',
                                             usecols=lambda c: not c.startswith('Unnamed:'), engine='c')
        else:
            self.dataframe = pd.read_csv(self.path, encoding=self.encoding, low_memory=False, header=self.file_header
                                         , dtype=str, na_values='', engine='c')
        self.dataframe_length = len(self.dataframe)

    def set_encoding(self, encoding: str) -> None:
        if encoding not in available_encodings:
            self.encoding = 'latin_1'
        else:
            self.encoding = encoding.lower().replace("-","_")

    def update_check_values_and_regex(self) -> None:
        self.check_chars = list(set(self.check_char_user_input.split(" ")))
        # in case the user has a trailing space we get an empty string in the list, this removes that:
        if '' in self.check_chars:
            self.check_chars.remove('')
        new_chars = []
        # escape characters which need to be escaped in regex (pop old one from list, add escaped version)
        for c in self.check_chars:
            if c in characters_to_escape_in_regex:
                c_new = escape(c)
                self.check_chars.remove(c)
                new_chars.append(c_new)
        self.check_chars += new_chars
        # build regex with or (|) from character list > this will match all characters specified
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
            loop = get_running_loop()
            for col in self.dataframe:
                filtered_df = await loop.run_in_executor(
                    executor,
                    # search the dataframe for the specified regex build from user character choice
                    lambda col=col: self.dataframe[
                        self.dataframe[col].astype(str).str.contains(self.check_chars_regex, na=False, regex=True)]
                )
                # if the filtered dataset is not empty, we save its row count and percentage of total rows for later use
                if not filtered_df.empty:
                    len_df = len(filtered_df)
                    self.cols_with_char[col] = (len_df, f"{len_df/self.dataframe_length * 100:.2f}%")

    def get_filtered_rows(self, column_name: str ,head: int =10) -> pd.DataFrame:
        """returns the top x rows (head, default 10) of the dataframe filtered on column column_name and on the chars
        in self.check_chars_regex"""
        return self.dataframe[
            self.dataframe[column_name].astype(str).str.contains(self.check_chars_regex, na=False)].head(head)

    async def transform_file(self, char_out: str, char_in: str) -> None:
        char_out = escape(char_out)
        self.transformed_df = self.dataframe.replace({char_out: char_in}, regex=True)

    async def export_file(self, export_path: str, separator: str) -> None:
        timestamp = datetime.now().strftime('%Y_%m_%d %H_%M_%S')
        file_name = f"{self.path.stem}_{timestamp}"
        file_suffix = self.path.suffix # should be .csv anyhow
        export_path_with_file = f"{export_path}/{file_name}{file_suffix}"
        if self.file_header is None:
            has_header = False
        else:
            has_header = [str(col) for col in self.transformed_df.columns]
        # currently we loose the quoting around values if it is not needed, even when it is present in the initial
        # file. I am not sure if that is a plus or minus..
        # wrapped this in the same concurrent.futures routine as the analyzing above to not let the ui lose connection
        # connection loss still happens, but it seems that the UI can reconnect when we use this.
        with concurrent.futures.ThreadPoolExecutor() as executor:
            loop = get_running_loop()
            await loop.run_in_executor(
                executor,
                lambda: self.transformed_df.to_csv(
                            Path(export_path_with_file),
                            sep=separator,
                            header=has_header,
                            index=False,
                            encoding=self.encoding,
                            mode='x',
                            quotechar='"'
                )
            )


async def load_file_and_set_dataframe() -> None:
    analyze_button.set_visibility(False)
    analyze_button.update()
    data_table.set_visibility(False)
    data_label.set_visibility(False)
    result_table.set_visibility(False)
    try:
        file_path = await choose_file()
    except Exception as e:
        ui.notify(f"Path couldn't be set. \n {e}")
        return
    path_label.set_visibility(False)
    loading_spinner_file.set_visibility(True)
    # we need this so the control is yielded back to the event loop and the ui is updated, without this the spinner is
    # not shown.
    await sleep(0.1)
    try:
        fileHandler.set_file_path(file_path)
    except Exception as e:
        ui.notify(f"Path couldn't be set. \n {e}")
        return
    try:
        fileHandler.set_dataframe_from_filepath()
    except Exception as e:
        ui.notify(f"Dataframe couldn't be build. \n {e}")
        return
    analyze_button.set_visibility(True)
    analyze_button.update()
    loading_spinner_file.set_visibility(False)
    path_label.set_visibility(True)
    path_label.text = str(fileHandler.path)


async def reload_file_and_dataframe() -> None:
    if fileHandler.path is not None:
        analyze_button.set_visibility(False)
        analyze_button.update()
        path_label.set_visibility(False)
        loading_spinner_file.set_visibility(True)
        # we need this so the control is yielded back to the event loop and the ui is updated, without this the spinner is
        # not shown.
        await sleep(0.1)
        fileHandler.set_dataframe_from_filepath()
        analyze_button.set_visibility(True)
        analyze_button.update()
        loading_spinner_file.set_visibility(False)
        path_label.set_visibility(True)
        result_table.set_visibility(False)
        data_table.set_visibility(False)
        data_label.set_visibility(False)
    else:
        ui.notify("No file loaded.")


async def choose_file() -> str:
    file_types = ('CSV Files (*.csv)', 'All files (*.*)') # for some reason i can not only use the CSV part here. idk..
    # this always returns a list of files even when only 1 is chosen.
    # webview.OPEN_DIALOG, this was part of the arguments of create_file_dialog. but i cant install webview outside of
    #pycharm. it seems to be not needed!
    files = await app.native.main_window.create_file_dialog(allow_multiple=False, file_types=file_types)
    return files[0]


async def transform_and_save_file() -> None:
    export_spinner.set_visibility(True)
    download_and_swap_button.set_visibility(False)
    file_types = ('CSV Files (*.csv)', 'All files (*.*)')
    target_path = await app.native.main_window.create_file_dialog(allow_multiple=False, file_types=file_types, dialog_type=webview.FOLDER_DIALOG)
    target_path = target_path[0] # create_file_dialog returns a list, we only want the first entry
    try:
        await fileHandler.transform_file(swap_out_character.value, swap_in_character.value)
        await fileHandler.export_file(target_path, export_separator.value)
    except Exception as e:
        ui.notify(e)
        export_spinner.set_visibility(False)
        download_and_swap_button.set_visibility(True)
        return
    await sleep(0.1)
    export_spinner.set_visibility(False)
    download_and_swap_button.set_visibility(True)
    ui.notify("File exported successfully.")


def kill_script() -> None:
    """this should trigger on shutdown of the ui and kill the rest of the script. for some reason this does not work
    cleanly. currently this is only pass through function to sys.exit()."""
    exit()


async def analyzer_click() -> None:
    try:
        result_table.set_visibility(False)
        data_label.set_visibility(False)
        data_table.set_visibility(False)
        analyze_button.set_visibility(False)
        loading_spinner_analyzer.set_visibility(True)
        await fileHandler.analyze_dataframe()
        populate_result_table()
        loading_spinner_analyzer.set_visibility(False)
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
    data_label.set_visibility(True)
    panels.set_value('Data Preview')
    panels.update()


if __name__ in ("__main__", "__mp_main__"):
    app.on_shutdown(kill_script)  # sys exit is triggered here, for some reason that does not cleanly exit the script.. the script is exited by crashing though
    fileHandler = FileHandler('', '')

    # this smells to me and is a spaghetti. i need to refactor this to be more succinct.
    with ui.tabs().classes('w-full') as tabs:
        main_page = ui.tab('Main Tab')
        data_view = ui.tab('Data Preview')
        export_page = ui.tab('Export')
    with ui.tab_panels(tabs, value=main_page).classes('w-full') as panels:
        with ui.tab_panel(main_page):
            with ui.row():
                encoding_menu = ui.select(available_encodings, label='File encoding', with_input=True, value='latin_1')
                encoding_menu.bind_value(fileHandler, 'encoding')
                with ui.row():
                    file_has_headers = ui.checkbox("File has headers", value=True)
                    file_has_headers.on_value_change(lambda e: fileHandler.toggle_header_mode(e.value))
                    supress_unnamed = ui.checkbox("Supress unnamed rows", value=True)
                    supress_unnamed.bind_value(fileHandler, 'supress_unnamed_columns')
            with ui.row():
                choose_file_button = ui.button('choose file', on_click=load_file_and_set_dataframe)
                reload_file_Button = ui.button('reload file', on_click=reload_file_and_dataframe)

            with ui.expansion('file:', value=True).classes('w-full'):
                path_label = ui.label('--no file chosen--')
                loading_spinner_file = ui.spinner(size='lg')
                loading_spinner_file.set_visibility(False)

            # how can we make this so that we also update the value list and regex when this is changed
            check_character_input = ui.input(label='Character to check for', value=',')
            check_character_input.bind_value(fileHandler, 'check_char_user_input')
            check_character_input.tooltip("You can add multiple values by separating them with a space")
            check_character_input.on_value_change(fileHandler.update_check_values_and_regex)

            analyze_button = ui.button('analyze file', on_click=analyzer_click)
            analyze_button.set_visibility(False)

            loading_spinner_analyzer = ui.spinner(size='lg')
            loading_spinner_analyzer.set_visibility(False)

            result_table = ui.table(columns=[], rows=[])
            result_table.add_slot('body-cell-title', r'<td><a :href="props.row.url">{{ props.row.title }}</a></td>')
            result_table.on('rowClick', lambda e: show_data_rows(e.args[1]["column"]))
            result_table.set_visibility(False)
        with ui.tab_panel(data_view):
            data_label = ui.label('')
            data_label.set_visibility(False)
            data_table = ui.table(columns=[], rows=[])
            data_table.set_visibility(False)
        with ui.tab_panel(export_page):
            with ui.row():
                swap_out_character = ui.input(label='String to swap out', value=',')
                swap_in_character = ui.input(label='String to swap in', value='@$@$@')
                export_separator = ui.input(label='Separator', value=',')
            download_and_swap_button = ui.button('Swap string and save file', on_click=transform_and_save_file)
            export_spinner = ui.spinner(size='lg')
            export_spinner.set_visibility(False)

    ui.run(native=True,
           dark=True,
           reload=False,
           title='csv character analyzer',
           window_size=(500,800),
           favicon='CharacterCheckIcon_128.png')
    #favicon only works in browser mode, it seems we cant change the app icon in native mode.