import gym
import random
import math
env = gym.make('CartPole-v0')


def ob_index(o):
    binary = [1 if i > 0 else 0 for i in o]
    s = reduce(lambda s, i: "%s%i" % (s, i), binary)
    k = 0
    try:
        k = int(s, 2)
    except:
        print(binary, s)
    return k


R = {}
V = {}
sigma = 0.5
for i_episode in range(10):
    observation = env.reset()

    for t in range(100):
        env.render()

        best_action = 0
        i_ob = ob_index(observation)
        max_rewards = 0
        for (ob, rewards) in R.items():
            j_ob = ob[0]
            if j_ob == i_ob and rewards > max_rewards:
                best_action = ob[1]
                max_rewards = rewards
        rand_action = env.action_space.sample()
        action = rand_action
        if random.random() > sigma:
            action = best_action

        observation, reward, done, info = env.step(action)
        print(observation)
        index = "%d-%d" % (i_ob, action)
        rewards = R.get(index, 0)
        R[index] = rewards + reward
        if done:
            print("Episode finished after {} timesteps".format(t+1))
            break

print(R)