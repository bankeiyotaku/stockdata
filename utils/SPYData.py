import pandas as pd

def getSPYList(rd, index_date):
    spy_history = rd.get_data(universe=['.SPX'],
                        fields=['TR.IndexJLConstituentRIC',
                        'TR.IndexJLConstituentName.value',
                        'TR.IndexJLConstituentRIC.change',
                        'TR.IndexJLConstituentChangeDate.value'],
                        parameters={"SDate":"1990-01-01","EDate":"2023-04-05","IC":"B"})



    spy_history.sort_values(by = ['Date','Change'], ascending=[True, True], inplace=True)
    spy_history.set_index("Date", inplace=True)
    spy_history.drop(columns="Instrument",inplace=True)

    spy_on_date = {}

    spy_subset = spy_history.loc[ (spy_history.index.date <= index_date) ]
    spy_subset.sort_index()

    for index, row in spy_subset.iterrows():
        if index > index_date:
                print("INDEX!!")
        if row["Change"] == "Joiner":
            if row["Constituent RIC"] in spy_on_date:
                print("Joined twice:", row["Constituent RIC"])
                spy_on_date[row["Constituent RIC"]] += 1
            else:
                spy_on_date[ row["Constituent RIC"] ] = 1
        elif row["Change"] == "Leaver":
            if row["Constituent RIC"] in spy_on_date:
                if( spy_on_date[row["Constituent RIC"]] > 1 ):
                    print("Deleting added ref", row["Constituent RIC"])
                    spy_on_date[row["Constituent RIC"]] -= 1
                else:
                    del spy_on_date[row["Constituent RIC"]]
            else:
                print("Not found to remove!: ", row["Constituent RIC"])
        else:
            print("Leaver/Joiner badly formed:", row["Change"])

    spy_subset = list( spy_on_date.keys() )

    return (spy_subset)


