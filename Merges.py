import pandas as pd
#


def write_output(*args):
    return False
    StringCheck = False
    LineCheck = False
    for x in args:
        if (isinstance(x, str)):
            StringCheck = True
        else:
            StringCheck = False
            break
    for x in args:
        if StringCheck == False:
            write_output(x)
        else:
            LineCheck = True
            break
    if (LineCheck == True):
        write_output(" ".join(args))


def Merges(DataSetCSV, IterList, BaseTableSet, DaskCPU=2):
    # Defining Our Variables:
    CurrentDS = DataSetCSV
    EList = IterList
    ECounter = 0
    # These are Set inside
    NewBaseTable = BaseTableSet
    CurrentJoinType = "To Set"
    CurrentJoinTable = "To Set"
    LeftKey = "To Set"
    RightKey = "To Set"
    DuplicateKey = "To Set"
    CurrentFrameCounter = 0

    write_output("New Iteration")
    while ECounter < len(EList):
        try:
            # Init
            if EList[ECounter] == "Init":
                ECounter += 1
                write_output(ECounter)
                continue

            # Seperate into Modules:
            if EList[ECounter] == "BaseTableStart":
                ECounter += 1
                write_output("TableStart")
                ECounter += 1
                write_output(NewBaseTable)
                # We already set the first time base table

                if EList[ECounter] == "BaseTableEnd":
                    ECounter += 1
                    write_output("Base Table End")

                if EList[ECounter] == "BaseTableSheetStart":
                    ECounter += 2

                if EList[ECounter] == "BaseTableSheetEnd":
                    ECounter += 1
                continue

            # Seperate into Modules:
            if EList[ECounter] == "JoinTypeTrigger":
                ECounter += 1
                write_output("JoinTypeTrigger")

                if EList[ECounter] == "JoinType":
                    ECounter += 1
                    write_output("JoinType")
                    CurrentJoinType = str(EList[ECounter]).replace(" ", "").replace("Join", "").lower()
                    ECounter += 1
                    write_output(CurrentJoinType)

                if EList[ECounter] == "EndJoinType":
                    ECounter += 1
                    write_output("EndJoinType")

                if EList[ECounter] == "JoinTypeTriggerEnd":
                    ECounter += 1
                    write_output("EndJoinType")
                continue

            # Seperate into Modules:
            if EList[ECounter] == "AddFileTrigger":
                ECounter += 1
                write_output("AddFileTrigger")

                if EList[ECounter] == "SelectedFile":
                    ECounter += 1
                    write_output("SelectedFile")
                    CurrentJoinTable = CurrentDS[int(CurrentFrameCounter)]
                    CurrentFrameCounter += 1
                    ECounter += 1
                    write_output(CurrentJoinTable)

                if EList[ECounter] == "EndSelectedFile":
                    ECounter += 1
                    write_output("EndSelectedFile")

                if EList[ECounter] == "StartSheetSelect":
                    ECounter += 2

                if EList[ECounter] == "EndSheetSelect":
                    ECounter += 1

                if EList[ECounter] == "LeftKeyStart":
                    ECounter += 1
                    write_output("LeftKeyStart")
                    LeftKey = str(EList[ECounter])
                    ECounter += 1
                    write_output(LeftKey)
                if EList[ECounter] == "LeftKeyEnd":
                    ECounter += 1
                    write_output("LeftKeyEnd")

                if EList[ECounter] == "RightKeyStart":
                    ECounter += 1
                    write_output("RightKeyStart")
                    RightKey = str(EList[ECounter])
                    ECounter += 1
                    write_output(RightKey)
                if EList[ECounter] == "RightKeyEnd":
                    ECounter += 1
                    write_output("RightKeyEnd")

                if EList[ECounter] == "DuplicateKeyStart":
                    ECounter += 1
                    write_output("DuplicateKeyStart")
                    DuplicateKey = str(EList[ECounter])
                    ECounter += 1
                    write_output(DuplicateKey)
                if EList[ECounter] == "DuplicateKeyEnd":
                    ECounter += 1
                    write_output("RightKeyEnd")

                if EList[ECounter] == "EndAddFile":
                    ECounter += 1
                    write_output("End")

                # Now, we do the Merge before we Enact the Conditional
                if str(DuplicateKey) == "Yes":
                    write_output("Got to Duplicate")
                    write_output(CurrentJoinTable)
                    write_output(NewBaseTable)
                    NewBaseTable = pd.merge(NewBaseTable.to_string(), CurrentJoinTable.to_string(), left_on=str(LeftKey), right_on=str(RightKey), how=str(CurrentJoinType), suffixes=('_left', '_right'))
                    NewBaseTable.drop_duplicates(inplace=True)
                    write_output("New Base Table")
                    write_output(NewBaseTable)

                else:
                    write_output("Got to Non-Duplicate")
                    write_output(CurrentJoinTable)
                    write_output(NewBaseTable)
                    NewBaseTable = pd.merge(NewBaseTable.astype(str), CurrentJoinTable.astype(str), left_on=str(LeftKey), right_on=str(RightKey), how=str(CurrentJoinType), suffixes=('_left', '_right'))
                    write_output("New Base Table")

            continue
        except Exception as Error:
            write_output(Error)
            return {'Response': 'Error', 'Error': str(Error)}
    return {'ReturnTable': NewBaseTable}
