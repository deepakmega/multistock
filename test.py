import time

def main():
    start_time = time.time()
    a = 1
    b = 2
    for i in range((50000)):
        c = a+b
        a = a+1
        b = b+1
        print("A=", a, " B=", b," C=",c)

    print("--- %s seconds ---" % (time.time() - start_time))

    return


if __name__ == '__main__':
    main()