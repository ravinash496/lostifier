#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
from unittest.mock import patch, MagicMock
import lostifier.command as cmd


class LoadInvokerTest(unittest.TestCase):

    @patch('lostifier.command.LoadCommand')
    def test_exec_command(self, command):
        command.execute = MagicMock()
        command.execute.return_value = 'test'

        target: cmd.LoadInvoker = cmd.LoadInvoker()
        target.execute(command)

        command.execute.assert_called_once()


if __name__ == '__main__':
    unittest.main()
