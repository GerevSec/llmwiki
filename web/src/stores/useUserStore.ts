import { create } from 'zustand'
import { persist } from 'zustand/middleware'

type User = {
  id: string
  email: string
}

type UserState = {
  user: User | null
  accessToken: string | null
  authLoading: boolean
  setUser: (user: User | null) => void
  setAccessToken: (token: string | null) => void
  setAuthLoading: (loading: boolean) => void
  signOut: () => void
}

export const useUserStore = create<UserState>()(
  persist(
    (set) => ({
      user: null,
      accessToken: null,
      authLoading: true,
      setUser: (user) => set({ user }),
      setAccessToken: (accessToken) => set({ accessToken }),
      setAuthLoading: (authLoading) => set({ authLoading }),
      signOut: () => set({ user: null, accessToken: null, authLoading: false }),
    }),
    {
      name: 'supavault:user',
      partialize: (state) => ({ accessToken: state.accessToken }),
    },
  ),
)
