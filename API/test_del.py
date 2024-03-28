import os

def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"Файл {file_path} успешно удален.")
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except PermissionError:
        print(f"У вас нет разрешения на удаление файла {file_path}.")
    except Exception as e:
        print(f"Произошла ошибка при удалении файла {file_path}: {str(e)}")

if __name__ == "__main__":
    file_path = input("Введите путь к файлу, который нужно удалить: ")
    delete_file(file_path)