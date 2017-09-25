import statistics

def report(t):
    t_rmses = []
    e_rmses = []
    for z in t.split("\n"):
        if "Training RMSE" in z:
            t_rmses.append(float(z[-5:]))
        if "Evaluation RMSE" in z:
            e_rmses.append(float(z[-5:]))
    print("training:\t", "\t".join(["%.3f" % t for t in t_rmses]))
    print("evaluation:\t", "\t".join(["%.3f" % e for e in e_rmses]))
    print("training total", "%.3f" % statistics.mean(t_rmses))
    print("evaluation total", "%.3f" % statistics.mean(e_rmses))
