import time as tm
import pandas as pd


def getpeers(e, peering_col, E, ST, features):
    if peering_col < 0 or peering_col >= len(features):
        return set(E.keys())
    else:
        peering_feature = features[peering_col]

        if E.get(e) is None:
            return set(E.keys())
        elif E.get(e).get(peering_feature) is None:
            return set(E.keys())
        else:
            # values = set()
            values = E.get(e).get(peering_feature)
            # peers = set()
            for v in values:
                if ST.get(peering_feature + "; " + v) is None:
                    continue
                else:
                    peers = (ST.get(peering_feature + "; " + v))

            if peers is None or len(peers) < 3:
                return set(E.keys())

            return peers


def writeopk(e, candidates, k, peers):
    result = e + " [ "
    listaK = dict()

    for i in range(k):
        key = ''

        key = max(candidates, key=candidates.get)

        score = float("{:.3f}".format((candidates.get(key)) / (1.0 * len(peers))))
        result += str(i) + ": ¬(" + str(key) + ")= " + str(score) + " "
        candidates.update({key: -1.0})

        listaK.update({int(key[key.find(";") + 1:].strip()): float(score)})

    result = result + "]"
    all_values = listaK.values()
    max_value = max(all_values)
    keys = [key for key, value in listaK.items() if value == max_value]

    if len(keys) > 1 or len(keys) == 1:
        keys = keys[0]

    # print(result)
    return (int(e), keys)


class NegativeUsefulSampling():

    def __init__(self, triple, peering_col, k):
        self.triple = triple
        self.peering_col = peering_col
        self.k = k
        self.len = len(triple)


    def __len__(self):
        return self.len

    def timeit(method):
        def timed(*args, **kwargs):
            ts = tm.time()
            result = method(*args, **kwargs)
            te = tm.time()
            if 'log_time' in kwargs:
                name = kwargs.get('log_name', method.__name__.upper())
                kwargs['log_time'][name] = int((te - ts))
            else:
                print('%r  %2.3f s' % (method.__name__, (te - ts)))
            return result

        return timed

    def getTriple(self, triple):
        return self.triple

    #@timeit
    def inferenceNegativeUseful(self,triple, peering_col, k_):
        triple = self.triple
        ST = dict()
        E = dict()
        table_as_triples = set()
        features = ["Head", "Relation", "Tail"]

        data = pd.DataFrame(triple)
        data.columns = features

        positiveSamp = [(str(row[0]), str(row[1]), str(row[2])) for row in data[['Head', 'Relation', 'Tail']].to_numpy()]
        negativeSamp = list()

        for i in range(0, len(positiveSamp)):
            parts = positiveSamp[i]
            entity = parts[0]
            if len(entity) == 0:
                continue

            st = dict()

            if E.get(entity) is None:
                st = dict()
            else:
                st = E.get(entity)

            for j in range(1, len(parts)):
                if len(parts[j]) == 0 or parts[j] is None:
                    continue

                if st.get(features[j]) is None:
                    set_ = set()
                    comma = list()
                    comma.extend(parts[j].split(','))
                    for k in range(len(comma)):
                        set_.add(str(comma[k]))

                        st.update({features[j]: set_})

                else:
                    comma = list()
                    set_ = st.get(features[j])
                    comma.append(parts[j])
                    for k in range(len(comma)):
                        set_.add(comma[k])

                        st.update({features[j]: set_})

                E.update({entity: st})

                e = set()
                comma = list()
                comma.extend(parts[j].split(','))
                for z in range(len(comma)):
                    if ST.get(features[j] + "; " + comma[z]) is None:
                        e = set()
                        e.add(entity)
                    else:
                        e = ST.get(features[j] + "; " + str(comma[z]))
                        e.add(entity)

                    ST.update({features[j] + "; " + comma[z]: e})
                    table_as_triples.add(entity + features[j] + comma[z])

        # print("Inferring useful negations...")

        for e in E.keys():
            # peers = set()

            peers = getpeers(e, peering_col, E, ST, features)

            candidates = dict()
            for p in peers:
                if p == e:
                    continue

                st = E.get(p)

                for entryx, entryY in st.items():
                    x = entryx
                    for y in entryY:
                        if table_as_triples.__contains__(e + x + y):
                            continue
                        if candidates.get(x + "; " + y) is None:
                            if x == 'Relation':
                                continue
                            else:
                                candidates.update({x + "; " + y: 1})
                        else:
                            candidates.update({x + "; " + y: candidates.get(x + "; " + y) + 1})

            if len(candidates.keys()) != 0:
                negativeSamp.append(writeopk(e, candidates, k_, peers))
            else:
                continue

        # print("Check the 'output' folder!")

        # negativeSamp = list(set([(int(p[0]),int(p[1]),int(n[1])) for n in negativeSamp for p in positiveSamp if int(p[0]) == n[0]]))
        # print(len(negativeSamp))
        # negativeSamp = [(int(p[0]),int(p[1]),int(n[1])) for n in negativeSamp for p in positiveSamp if int(p[0]) == n[0]]

        # negativeSamp = [n for n_2 in negativeSamp for n in n_2]

        d1 = {int(p[0]): int(p[1]) for p in positiveSamp}
        d2 = {n[0]: n[1] for n in negativeSamp}

        intersection = d1.keys() & d2.keys()
        # print("fine ricerca elementi...")

        negativeSamp = list(intersection) + list(map(d1.get, intersection)) + list(map(d2.get, intersection))

        return negativeSamp



    def __iter__(self):
        return self.inferenceNegativeUseful(self.triple,self.peering_col,self.k)