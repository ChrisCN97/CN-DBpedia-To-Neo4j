import csv

#neo4j-admin import --database=CN-DBpedia.db --nodes="../import/node.csv" --relationships="../import/relation.csv" --multiline-fields=true

if __name__ == "__main__":
    # 改为cn-dbpedia的数据
    dataPath = "../data/sample.txt"

    resourceFile = open(dataPath, encoding='utf-8')
    nodeFile = open("../data/node.csv", "w+", newline='', encoding='utf-8')
    relationFile = open("../data/relation.csv", "w+", newline='', encoding='utf-8')
    nodeCsv = csv.writer(nodeFile)
    relationCsv = csv.writer(relationFile)

    # node.csv中最好有:LABEL属性
    nodeCsv.writerow([":ID", "name", ":LABEL"])
    # 不能将relation的名字直接设置为TYPE，不然TYPE会达到数量上限
    relationCsv.writerow([":START_ID", ":END_ID", "name", ":TYPE"])
    nodeDic = dict()
    num = 1

    line = resourceFile.readline()
    while line is not None and line != '':
        n1, r, n2 = line.split('\t')
        # 该图谱最长的节点name长达19759，后期建立索引会失败，可以在这里把过长的节点或关系剔除
        if len(n1)>15 or len(n2)>15 or len(r)>15:
            line = resourceFile.readline()
            continue
        if n1 not in nodeDic:
            nodeDic[n1] = num
            nodeCsv.writerow([num, n1, "node"])
            num += 1
        if n2 not in nodeDic:
            nodeDic[n2] = num
            nodeCsv.writerow([num, n2, "node"])
            num += 1
        relationCsv.writerow([nodeDic[n1], nodeDic[n2], r, "relation"])

        line = resourceFile.readline()

    nodeFile.close()
    relationFile.close()