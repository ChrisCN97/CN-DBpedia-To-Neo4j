from py2neo import Graph
import csv
import random

"""
node: ":LABEL", "name", ":ID"
copyright: ":LABEL", "copyright_id", ":ID", "complete_time", "software_name"
project: ":LABEL", "acc_id", ":ID", "p_institute", "p_name"
teacher: ":LABEL", "t_id", ":ID", "department", "id", "t_institute", "t_name"  # num:25820

relation: ":START_ID", ":END_ID", "name", ":TYPE"

./neo4j-admin import --database=neo4j-reduce --nodes="../import/node.csv" --nodes="../import/copyright.csv" --nodes="../import/project.csv" --nodes="../import/teacher.csv" --relationships="../import/relation.csv" --multiline-fields=true
"""

def getType(node):
    nodeType = ""
    for s in node.labels._SetView__collection:
        nodeType = s
    return nodeType

def writeCsvDic(node, nodeType, csvDic):
    if nodeType == "node":
        name = node["name"]
        if name not in csvDic["node"]:
            csvDic["node"][name] = csvDic["id"]
            csvDic["id"] += 1
        return csvDic["node"][name]
    if nodeType == "copyright":
        copyright_id = node["copyright_id"]
        if copyright_id not in csvDic["copyright"]:
            csvDic["copyright"][copyright_id] = [csvDic["id"], node["complete_time"], node["software_name"]]
            csvDic["id"] += 1
        return csvDic["copyright"][copyright_id][0]
    if nodeType == "project":
        acc_id = node["acc_id"]
        if acc_id not in csvDic["project"]:
            csvDic["project"][acc_id] = [csvDic["id"], node["p_institute"], node["p_name"]]
            csvDic["id"] += 1
        return csvDic["project"][acc_id][0]
    if nodeType == "teacher":
        t_id = node["t_id"]
        if t_id not in csvDic["teacher"]:
            csvDic["teacher"][t_id] = [csvDic["id"], node["department"], node["id"], node["t_institute"], node["t_name"]]
            csvDic["id"] += 1
        return csvDic["teacher"][t_id][0]
    if nodeType == "relation":
        if node not in csvDic["relation"]:
            csvDic["relation"].add(node)

def neoSearchByType(nodeType, csvDic, travelStep, batch, graph, test=0, nodeNeiSize=5000):
    iterNum = 0
    data = graph.run("MATCH (p:"+nodeType+") RETURN count(p)").data()
    total = int(data[0]['count(p)'])
    cur = 1
    while True:
        data = graph.run("match (p:"+nodeType+") RETURN p SKIP "+str(test+iterNum*batch)+' LIMIT '+str(batch)).data()
        iterNum += 1
        if len(data) == 0:
            break
        for rec in data:
            print("******{} {}/{}".format(nodeType, cur, total))
            cur += 1
            neoSearchById(rec['p'].identity, csvDic, travelStep, batch, graph, nodeNeiSize)

def neoSearchById(id, csvDic, travelStep, batch, graph, nodeNeiSize):
    if travelStep == 0:
        return
    iterNum = 0
    data = graph.run("MATCH (p)--(q) where id(p)="+str(id)+" RETURN count(q)").data()
    total = int(data[0]['count(q)'])
    cur = 1
    while True:
        data = graph.run('MATCH (p)-[r]-(q) where id(p)='+str(id)+' RETURN p,r,q '
                         +'SKIP '+str(iterNum*batch)+' LIMIT '+str(batch)).data()  # cypher
        iterNum += 1
        if len(data) == 0:
            break
        for rec in data:
            if total > nodeNeiSize:
                if random.random() > nodeNeiSize/float(total):
                    if cur % 1000 == 1:
                        print("{} {}/{}".format(id, cur, total))
                    cur += 1
                    continue
            pid = writeCsvDic(rec['p'], getType(rec['p']), csvDic)
            csvDic["haveTravel"].add(rec['p'].identity)
            qid = writeCsvDic(rec['q'], getType(rec['q']), csvDic)
            writeCsvDic((pid, qid, rec['r']['name']), "relation", csvDic)
            if rec['q'].identity not in csvDic["haveTravel"]:
                if total > 1000:
                    if cur % 1000 == 1:
                        print("{} {}/{}".format(id, cur, total))
                    cur += 1
                csvDic["haveTravel"].add(rec['q'].identity)
                neoSearchById(rec['q'].identity, csvDic, travelStep-1, batch, graph, nodeNeiSize)

def control():
    graph = Graph("http://localhost:7474/", auth=("neo4j", "se1405"))
    csvDic = {
        "node": dict(),
        "copyright": dict(),
        "project": dict(),
        "teacher": dict(),
        "relation": set(),
        "id": 1,
        "haveTravel": set()
    }
    neoSearchByType(nodeType="copyright", csvDic=csvDic, travelStep=3, batch=50, graph=graph, nodeNeiSize=500)
    neoSearchByType(nodeType="project", csvDic=csvDic, travelStep=3, batch=50, graph=graph, nodeNeiSize=500)
    neoSearchByType(nodeType="teacher", csvDic=csvDic, travelStep=3, batch=50, graph=graph, nodeNeiSize=500)
    print("\n\nTotal Node:{} Total Relation:{}".format(csvDic["id"], len(csvDic["relation"])))

    nodeFile = open("node.csv", "w+", newline='', encoding='utf-8')
    copyrightFile = open("copyright.csv", "w+", newline='', encoding='utf-8')
    projectFile = open("project.csv", "w+", newline='', encoding='utf-8')
    teacherFile = open("teacher.csv", "w+", newline='', encoding='utf-8')
    relationFile = open("relation.csv", "w+", newline='', encoding='utf-8')

    nodeCsv = csv.writer(nodeFile)
    copyrightCsv = csv.writer(copyrightFile)
    projectCsv = csv.writer(projectFile)
    teacherCsv = csv.writer(teacherFile)
    relationCsv = csv.writer(relationFile)

    nodeCsv.writerow([":LABEL", "name", ":ID"])
    copyrightCsv.writerow([":LABEL", "copyright_id", ":ID", "complete_time", "software_name"])
    projectCsv.writerow([":LABEL", "acc_id", ":ID", "p_institute", "p_name"])
    teacherCsv.writerow([":LABEL", "t_id", ":ID", "department", "id", "t_institute", "t_name"])
    relationCsv.writerow([":START_ID", ":END_ID", "name", ":TYPE"])

    for name in csvDic["node"]:
        nodeCsv.writerow(["node", name, csvDic["node"][name]])
    for copyright_id in csvDic["copyright"]:
        t = ["copyright", copyright_id]
        t.extend(csvDic["copyright"][copyright_id])
        copyrightCsv.writerow(t)
    for acc_id in csvDic["project"]:
        t = ["project", acc_id]
        t.extend(csvDic["project"][acc_id])
        projectCsv.writerow(t)
    for t_id in csvDic["teacher"]:
        t = ["teacher", t_id]
        t.extend(csvDic["teacher"][t_id])
        teacherCsv.writerow(t)
    for p, q, r in csvDic["relation"]:
        relationCsv.writerow([p, q, r, "relation"])

    nodeFile.close()
    copyrightFile.close()
    projectFile.close()
    teacherFile.close()
    relationFile.close()

if __name__ == "__main__":
    control()
