class reusable:

    def dropColumns(self, df, columns):
        df.drop(*columns)
        return df