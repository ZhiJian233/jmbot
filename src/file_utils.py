import os
from pathlib import Path
from typing import Iterator, Union, Optional, List
import asyncio

async def list_files_iter(
    directory: Union[str, Path],
    recursive: bool = True,
    include_dirs: bool = False,
    extensions: Optional[List[str]] = None
) -> Iterator[str]:
    """
    返回文件夹下所有文件名的迭代器（按字母顺序排序）

    Args:
        directory: 目标文件夹路径
        recursive: 是否递归子文件夹，默认True
        include_dirs: 是否包含文件夹，默认False
        extensions: 文件扩展名过滤列表，如['.jpg', '.png']，默认None表示不过滤

    Yields:
        str: 文件的完整路径（已排序）
    """
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"目录不存在: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"路径不是目录: {directory}")

    if recursive:
        pattern = "**/*"
    else:
        pattern = "*"

    # 收集所有符合条件的路径
    paths = []
    for path in directory.glob(pattern):
        if not include_dirs and path.is_dir():
            continue

        if extensions and path.is_file():
            if path.suffix.lower() not in [ext.lower() for ext in extensions]:
                continue

        paths.append(str(path.absolute()))

    # 排序后返回迭代器
    for path in sorted(paths):
        yield path

async def list_files_relative(
    directory: Union[str, Path],
    recursive: bool = True,
    include_dirs: bool = False,
    extensions: Optional[List[str]] = None
) -> Iterator[str]:
    """
    返回文件夹下所有文件名的迭代器（相对路径）

    Args:
        directory: 目标文件夹路径
        recursive: 是否递归子文件夹，默认True
        include_dirs: 是否包含文件夹，默认False
        extensions: 文件扩展名过滤列表，如['.jpg', '.png']，默认None表示不过滤

    Yields:
        str: 文件的相对路径
    """
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"目录不存在: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"路径不是目录: {directory}")

    if recursive:
        pattern = "**/*"
    else:
        pattern = "*"

    for path in directory.glob(pattern):
        if not include_dirs and path.is_dir():
            continue

        if extensions and path.is_file():
            if path.suffix.lower() not in [ext.lower() for ext in extensions]:
                continue

        yield str(path.relative_to(directory))

async def list_files_name_only(
    directory: Union[str, Path],
    recursive: bool = True,
    include_dirs: bool = False,
    extensions: Optional[List[str]] = None
) -> Iterator[str]:
    """
    返回文件夹下所有文件名的迭代器（仅文件名）

    Args:
        directory: 目标文件夹路径
        recursive: 是否递归子文件夹，默认True
        include_dirs: 是否包含文件夹，默认False
        extensions: 文件扩展名过滤列表，如['.jpg', '.png']，默认None表示不过滤

    Yields:
        str: 文件名（包含扩展名）
    """
    directory = Path(directory)

    if not directory.exists():
        raise FileNotFoundError(f"目录不存在: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"路径不是目录: {directory}")

    if recursive:
        pattern = "**/*"
    else:
        pattern = "*"

    for path in directory.glob(pattern):
        if not include_dirs and path.is_dir():
            continue

        if extensions and path.is_file():
            if path.suffix.lower() not in [ext.lower() for ext in extensions]:
                continue

        yield path.name

# 快捷函数
def list_all_files(directory: Union[str, Path]) -> Iterator[str]:
    """返回文件夹下所有文件的完整路径"""
    return list_files_iter(directory, recursive=True, include_dirs=False)

def list_all_files_relative(directory: Union[str, Path]) -> Iterator[str]:
    """返回文件夹下所有文件的相对路径"""
    return list_files_relative(directory, recursive=True, include_dirs=False)

def list_all_files_name(directory: Union[str, Path]) -> Iterator[str]:
    """返回文件夹下所有文件的文件名"""
    return list_files_name_only(directory, recursive=True, include_dirs=False)

if __name__ == "__main__":
    # 测试代码
    test_dir = "."
    print("完整路径:")
    for file_path in list_all_files(test_dir):
        print(f"  {file_path}")

    print("\n相对路径:")
    for file_path in list_all_files_relative(test_dir):
        print(f"  {file_path}")

    print("\n文件名:")
    for file_name in list_all_files_name(test_dir):
        print(f"  {file_name}")

    print("\n仅.py文件:")
    for py_file in list_files_iter(test_dir, extensions=['.py']):
        print(f"  {py_file}")
