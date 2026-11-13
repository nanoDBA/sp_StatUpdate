import unittest

from diag_commandlog_contract import check_sources, insert_column_sets


class DiagCommandLogContractTests(unittest.TestCase):
    def test_insert_columns_ignore_comments_and_bracket_names(self):
        sql = "INSERT INTO dbo.CommandLog ([CommandType], [ExtendedInfo]) VALUES (N'SP_STATUPDATE_START', N''); -- EndTime\n"
        self.assertEqual(insert_column_sets(sql), {"commandtype", "extendedinfo"})

    def test_reader_type_must_exist_in_writer_and_fixture(self):
        writer = "INSERT INTO dbo.CommandLog (CommandType, ExtendedInfo) VALUES (N'SP_STATUPDATE_START', N'');"
        fixture = writer
        diag = "SELECT c.ExtendedInfo FROM dbo.CommandLog AS c WHERE c.CommandType = N'SP_STATUPDATE_END';"
        gaps = check_sources(writer, diag, fixture)
        self.assertTrue(any("writer does not emit" in gap and "SP_STATUPDATE_END" in gap for gap in gaps))
        self.assertTrue(any("synthetic history" in gap and "SP_STATUPDATE_END" in gap for gap in gaps))

    def test_reader_element_must_exist_in_fixture(self):
        writer = "INSERT INTO dbo.CommandLog (CommandType, ExtendedInfo) VALUES (N'SP_STATUPDATE_START', N'<Parameters><RunLabel>x</RunLabel></Parameters>');"
        fixture = "INSERT INTO dbo.CommandLog (CommandType, ExtendedInfo) VALUES (N'SP_STATUPDATE_START', N'<Parameters><Version>x</Version></Parameters>');"
        diag = "SELECT c.ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)') FROM dbo.CommandLog AS c;"
        gaps = check_sources(writer, diag, fixture)
        self.assertTrue(any("element 'runlabel' absent from synthetic history" in gap for gap in gaps))

    def test_reader_column_must_be_inserted(self):
        writer = "INSERT INTO dbo.CommandLog (CommandType, ExtendedInfo) VALUES (N'SP_STATUPDATE_START', N'');"
        fixture = writer
        diag = "SELECT c.StartTime FROM dbo.CommandLog AS c;"
        gaps = check_sources(writer, diag, fixture)
        self.assertTrue(any("column 'starttime'" in gap for gap in gaps))

    def test_reader_visible_stop_reason_must_be_in_fixture(self):
        writer = "SET @StopReasonOut=N'COMPLETED';"
        fixture = "<StopReason>TIME_LIMIT</StopReason>"
        diag = "SELECT CASE WHEN x = N'COMPLETED' THEN 1 END FROM dbo.CommandLog AS c;"
        gaps = check_sources(writer, diag, fixture)
        self.assertTrue(any("StopReason 'COMPLETED' absent from synthetic history" in gap for gap in gaps))


if __name__ == "__main__":
    unittest.main()
