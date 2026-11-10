-- These delimiters are text in a line comment: /* not nested */
DECLARE @text nvarchar(50) = N'/* not nested */';
SELECT [identifier/*text*/] AS quoted_name;
/* A single ordinary block comment is valid. */
GO
