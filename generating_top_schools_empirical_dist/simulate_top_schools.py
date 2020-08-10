# Generating top 16 schools
# This Program was written by Yubo Wang. It contains jupyter notebook code for
# generating the empirical distribution in the XC Running.

# General Question:
# To generate the empirical distribution, we need to simulate the frequency of
# each school being into the top 16 in the state tournament.

# Our Approach:
# We simulate the per mile pace for each running in each school then based on
# the per mile pace, we rank the runners and choose the top runners from each
# school. Then we calculate the score for each school and then get the top 16
# schools in the state tournament.


import multiprocess as mp
import sys
import csv


def run_all(num, p):
    model_alpha_parameter = 2.5
    model_beta_parameter = 4
    model_scale_parameter = 19
    model_shift_parameter = 5.7

    import numpy as np
    import pandas as pd
    import math


    def drawOneRunner ():
        '''
        Use the beta model to draw one female runner
        '''
        pace = np.random.beta(model_alpha_parameter, model_beta_parameter)
        pace = pace * model_scale_parameter + model_shift_parameter
        return pace


    def drawPopulation (n):
        '''
        Use the beta model to draw a population of n females
        '''
        paces = np.array(np.random.beta(model_alpha_parameter,model_beta_parameter,size=n))
        paces = paces * model_scale_parameter + model_shift_parameter
        return paces


    def drawTeam (n,teamsize=7):
        '''
        Draw a team of top 7 runners from a population of n
        '''
        pop = drawPopulation(n)
        ret = np.partition(pop, teamsize)[:teamsize]
        return ret


    def f2time (f):
        '''
        convert float f from decimal time to clock time
        '''
        sec = int(60*(f-math.floor(f)))
        s = str(int(f)) + ':' + str(sec).zfill(2)
        return s
        

    div1 = pd.read_csv('div1.csv')
    div2 = pd.read_csv('div2.csv')
    div3 = pd.read_csv('div3.csv')
    allxc = pd.read_csv('allxc.csv')


    def randomAssignment ( df ):
        '''
        Take in a dataframe and return a new data frame with random assignment
        of region and district
        '''
        df2 = df.copy()
        IDlist = df2['ID'].tolist()
        df2['Region'] = 0
        df2['District'] = 0
        n = len(df2)
        perm = np.random.permutation(n)
        for i, j in zip(perm, range(n)):
            df2.iloc[i, 2] = j % 3 + 1 ## dist
            df2.iloc[i, 1] = (j // 3) % 4 + 1 ## regi
        return df2


    def createSeason(season_roster,df):
        '''
        season_roster is a list of team IDs that are participating
        in this season.
        
        df is a dataframe containing 'ID' field and 'pop' field with 
        school population
        
        returns a dictionary of 
            key = team ID
            value = list of 7 runners average paces
        '''
        teamRunners = {}
        for team in season_roster:
            pop = df[df.ID == team].iloc[0]['pop']
            teamPaces = drawTeam(pop)
            teamRunners[team] = teamPaces
        
        return teamRunners


    def simulateRunner (pace):
        '''
        A single runner has variation in their pace.
        This function uses a random distribution to adjust their pace.
        '''
        a = np.float(2.5)
        b = np.float(6)
        paces = np.float(np.random.beta(a,b))
        return paces * 0.7 - 0.15 + pace


    def simulateTeamRace (team):
        '''
        team is a list of paces for the team
        We return a list of finishing times for each runner. 
        '''
        finish = np.empty(len(team))
        for pace, i in zip(team, range(len(team))):
            finish[i] = 3.1069 * simulateRunner(pace)
        finish.sort()
        return finish


    def simulateMeet (teamList, teamData):
        '''
        teamList is a list of team IDs participating in this meet
        teamData is a dictionary
            key = ID
            value = list of avg paces by runner in team 
        Returns a pandas dataframe of simulated meet results
            The dataframe has a 'Team' column containing the team ID for a runner
            and a 'Time' column containing a float time for that runner's finishing time
        '''
        runnerName = []
        runnerResult = []
        
        for team in teamList:
            roster = teamData[team]       # get list of avg times for team runners
            times = simulateTeamRace(roster)
            for result in times:
                runnerName.append(team)
                runnerResult.append(result)
            
        
        results = pd.DataFrame( {'Team':runnerName, 'Time':runnerResult})
        return results
        

    def scoreMeet (meetResults):
        '''
        The input is a dataframe (the return value from simulateMeet)
        
        The return value is another dataframe with 'ID' and 'Score' columns, one row per team
        '''
        resdf = meetResults
        resdf['Place'] = resdf['Time'].rank()
        resdf['TeamPlace'] = resdf.groupby(by='Team')['Time'].rank()
        resdf = resdf.sort_values('Team')
        resdf['Score'] = 0
        resdf['Score'] = np.where(resdf['TeamPlace'] <= 5, resdf.Place, 0)
        teamScore = resdf.groupby(by='Team')['Score'].agg('sum').sort_values()
        return teamScore


    def simulateOneSeason(div):

        sdf = div[['ID']]
        sdf = randomAssignment(sdf)

        IDlist = div['ID'].tolist()
        teams = createSeason(IDlist,div)    


        stateRoster = []   # list of IDs making it to state meet
        for region in range(1,5):
            
            regionalRoster = []   # list of IDs making it to each region
            for district in range(1,4):
                
                districtRoster = sdf.loc[(sdf['Region']==region)&(sdf['District']==district)]['ID'].tolist()
                
                meetResult = simulateMeet(districtRoster,teams)
                meetScore = scoreMeet(meetResult)
                top5 = meetScore.iloc[:5].index.values.tolist()
                
                regionalRoster = regionalRoster + top5

            meetResult = simulateMeet(regionalRoster,teams)
            meetScore = scoreMeet(meetResult)
            top5 = meetScore.iloc[:5].index.values.tolist()
            stateRoster = stateRoster + top5
        meetResult = simulateMeet(stateRoster,teams)
        meetScore = scoreMeet(meetResult)
        
        return meetScore


    def run(num, p):

        ret = []
        percent = 0

        for i in range(num):

            if i%(num/100) == num/100 - 1:
                percent += 1
                print("process {}: {}%".format(p, percent))

            meetScore = simulateOneSeason(div1)
            teams = meetScore.index.values.tolist()
            ret.append(teams)
        return ret
    
    return run(num, p)


if __name__ == "__main__":  
    pool = mp.Pool(processes=mp.cpu_count())

    temp = []
    result = []

    for i in range(int(sys.argv[1])):
        temp.append(pool.apply_async(run_all, (int(sys.argv[2]), i,)))

    pool.close()
    pool.join()
    
    for i in temp[:]:
        for j in i.get():
            result.append(j)
    
    with open(sys.argv[3], "w", newline='\n') as csvfile:
        writer = csv.writer(csvfile)
        for row in result:
            writer.writerow(row[:16])