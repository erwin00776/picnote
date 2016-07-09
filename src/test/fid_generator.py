
def generate_fid(mark, start=1, count=100):
    if start < 1:
        start = 1
    nums = []
    cur = start
    while len(nums) < count:
        while cur & mark == 0:
            cur += 1
        nums.append(cur)
        cur += 1
    return nums

if __name__ == '__main__':
    print(generate_fid(0x2, 1, 20))
    print(generate_fid(0x4, 1, 20))
    print(generate_fid(0x8, 1, 20))